package structx

import (
	"encoding/binary"
	"math"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/exp/slices"
)

const (
	startBits  = 32
	offsetBits = 31
	ttlBits    = 8
	offsetMask = 0xffffffff

	defaultBufSize    = 1024
	compressThreshold = 0.5
)

var (
	order = binary.LittleEndian
)

// Idx is the index of BigCahce.
// start(32)|offset(31)|hasTTL(1)
type Idx uint64

func (i Idx) start() int {
	return int(i >> startBits)
}

func (i Idx) offset() int {
	return int((i & offsetMask) >> 1)
}

func (i Idx) hasTTL() bool {
	return i&1 == 1
}

func newIdx(start, offset int, hasTTL bool) Idx {
	// bound check
	if start > math.MaxUint32 || offset > math.MaxUint32 {
		panic("index overflow")
	}

	idx := Idx(start<<startBits | offset<<1)
	if hasTTL {
		idx |= 1
	}
	return idx
}

type BigCache struct {
	total int
	ts    int64
	buf   []byte
	idx   Map[string, Idx]
	sync.RWMutex
}

// NewBigCache returns a new BigCache.
func NewBigCache() *BigCache {
	c := &BigCache{
		ts:  time.Now().UnixNano(),
		buf: make([]byte, 0, defaultBufSize),
		idx: NewMap[string, Idx](),
	}
	go c.eliminate()

	return c
}

// Set set key-value pairs.
func (c *BigCache) Set(key string, value []byte) {
	c.Lock()
	defer c.Unlock()

	c.idx.Set(key, newIdx(len(c.buf), len(value), false))
	c.buf = append(c.buf, value...)

	c.total++
}

// SetEx set expiry time with key-value pairs.
func (c *BigCache) SetEx(key string, value []byte, dur time.Duration) {
	c.SetTx(key, value, c.ts+int64(dur))
}

// SetTx set deadline with key-value pairs.
func (c *BigCache) SetTx(key string, value []byte, ts int64) {
	c.Lock()
	defer c.Unlock()

	c.idx.Set(key, newIdx(len(c.buf), len(value), true))
	c.buf = append(c.buf, value...)
	c.buf = order.AppendUint64(c.buf, uint64(ts))

	c.total++
}

// Get
func (c *BigCache) Get(key string) ([]byte, bool) {
	value, _, ok := c.GetTx(key)
	return value, ok
}

// GetTx
func (c *BigCache) GetTx(key string) ([]byte, int64, bool) {
	c.RLock()
	defer c.RUnlock()

	return c.get(key)
}

func (c *BigCache) get(key string) ([]byte, int64, bool) {
	if idx, ok := c.idx.Get(key); ok {
		start := idx.start()
		end := start + idx.offset()

		// has ttl
		if idx.hasTTL() {
			ttl := int64(order.Uint64(c.buf[end : end+ttlBits]))

			// not expired
			if c.timeAlive(ttl) {
				return slices.Clone(c.buf[start:end]), ttl, true

			} else {
				c.idx.Delete(key)
			}

		} else {
			return slices.Clone(c.buf[start:end]), noTTL, true
		}
	}

	return nil, -1, false
}

// Remove
func (c *BigCache) Remove(key string) bool {
	c.Lock()
	defer c.Unlock()

	_, ok := c.idx.Delete(key)
	return ok
}

func (c *BigCache) timeAlive(ttl int64) bool {
	return ttl > c.ts || ttl == noTTL
}

// eliminate the expired key-value pairs.
func (c *BigCache) eliminate() {
	for c != nil {
		time.Sleep(time.Millisecond)
		c.Lock()

		// update ts
		start := time.Now()
		c.ts = start.UnixNano()

		for {
			// eliminate eval
			var pb, elimi float64

			for i := 0; i < probeCount; i++ {
				k, idx, ok := c.idx.GetPos(uint64(c.ts) + uint64(i*probeSpace))

				// expired
				if ok && idx.hasTTL() {
					end := idx.start() + idx.offset()
					ttl := int64(*(*uint64)(unsafe.Pointer(&c.buf[end])))

					if !c.timeAlive(ttl) {
						elimi++
						c.idx.Delete(k)
					}
				}
				pb++
			}

			// update ts
			ts := time.Now()
			c.ts = ts.UnixNano()

			// break if cost over limit or blow expRate
			if ts.Sub(start).Milliseconds() > eliminateMaxMs || elimi/pb <= expRate {
				break
			}
		}
		c.Unlock()

		// on compress threshold
		if float64(c.idx.Len())/float64(c.total) < compressThreshold {
			c.Compress()
		}
	}
}

// Compress
func (c *BigCache) Compress() {
	c.Lock()
	defer c.Unlock()

	c.total = 0
	nbuf := make([]byte, 0, len(c.buf))

	c.idx.Scan(func(key string, idx Idx) bool {
		// offset only contains value, except ttl
		start, offset, has := idx.start(), idx.offset(), idx.hasTTL()

		// reset
		c.idx.Set(key, newIdx(len(nbuf), offset, has))
		if has {
			nbuf = append(nbuf, c.buf[start:start+offset+ttlBits]...)
		} else {
			nbuf = append(nbuf, c.buf[start:start+offset]...)
		}

		c.total++
		return true
	})

	c.buf = nbuf
}

// Len
func (c *BigCache) Len() int {
	c.RLock()
	defer c.RUnlock()

	return c.idx.Len()
}
