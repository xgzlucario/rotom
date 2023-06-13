package structx

import (
	"bytes"
	"encoding/binary"
	"math"
	"sync"
	"time"
)

const (
	startBitsize  = 32
	offsetBitsize = 31
	offsetMask    = 0xffffffff
	ttlBitsize    = 8

	timeCarry         = 1000 * 1000
	defaultBufferSize = 1024
	compressThreshold = 0.5
)

// Idx is the index of BigCahce.
// start(32)|offset(31)|hasTTL(1)
type Idx uint64

func (i Idx) start() uint32 {
	return uint32(i >> startBitsize)
}

func (i Idx) offset() uint32 {
	return uint32(i & offsetMask >> 1)
}

func (i Idx) hasTTL() bool {
	return i&1 == 1
}

func newIdx(start, offset, hasTTL uint64) Idx {
	if start > math.MaxUint32 {
		panic("start index overflow")
	}
	return Idx(start<<startBitsize | offset<<1 | hasTTL&1)
}

type BigCache struct {
	total int // total size in buf
	ts    int64

	buf []byte
	idx Map[string, Idx]
	sync.RWMutex
}

// NewBigCache returns a new BigCache.
func NewBigCache() *BigCache {
	c := &BigCache{
		ts:  time.Now().UnixNano(),
		buf: make([]byte, 0, defaultBufferSize),
		idx: NewMap[string, Idx](),
	}
	go c.eliminate()

	return c
}

// Set
func (c *BigCache) Set(key string, value []byte) {
	c.Lock()
	defer c.Unlock()

	c.idx.Set(key, newIdx(uint64(len(c.buf)), uint64(len(value)), 0))
	c.buf = append(c.buf, value...)

	c.total++
}

// SetEx
func (c *BigCache) SetEx(key string, value []byte, dur time.Duration) {
	c.SetTx(key, value, c.ts+int64(dur))
}

// SetTx
func (c *BigCache) SetTx(key string, value []byte, ts int64) {
	c.Lock()
	defer c.Unlock()

	// Set idx and value.
	c.idx.Set(key, newIdx(uint64(len(c.buf)), uint64(len(value)), 1))
	c.buf = append(c.buf, value...)

	// Set ts.
	c.buf = binary.AppendVarint(c.buf, ts/timeCarry)
	c.buf = append(c.buf, 0)

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

	if idx, ok := c.idx.Get(key); ok {
		s := idx.start()

		// has ttl
		if idx.hasTTL() {
			i := s + idx.offset()
			ti := bytes.IndexByte(c.buf[i:], 0)
			ttl, _ := binary.Varint(c.buf[i : ti+int(i)])

			// not expired
			if c.timeAlive(ttl) {
				return c.buf[s:i], ttl * timeCarry, true
			}

		} else {
			return c.buf[s : s+idx.offset()], noTTL, true
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

// Len
func (c *BigCache) Len() int {
	c.RLock()
	defer c.RUnlock()

	return c.idx.Len()
}

func (c *BigCache) timeAlive(ttl int64) bool {
	return ttl > c.ts || ttl == noTTL
}

// eliminate the expired key-value pairs.
func (c *BigCache) eliminate() {
	for c != nil {
		time.Sleep(time.Second)
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
					i := idx.start() + idx.offset()
					ti := bytes.IndexByte(c.buf[i:], 0)
					ttl, _ := binary.Varint(c.buf[i : ti+int(i)])

					if !c.timeAlive(ttl) {
						elimi++
						c.idx.Delete(k)

						// compress threshold
						if float64(c.idx.Len()/c.total) < compressThreshold {
							c.Unlock()
							c.Compress()
							goto END
						}
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
	END:
	}
}

// Compress
func (c *BigCache) Compress() {
	c.Lock()
	defer c.Unlock()

	// initial
	c.total = 0
	nbuf := make([]byte, 0, defaultBufferSize)

	c.idx.Scan(func(key string, idx Idx) bool {
		start := idx.start()
		end := start + idx.offset()

		nbuf = append(nbuf, c.buf[start:end]...)
		c.total++

		if idx.hasTTL() {
			ti := bytes.IndexByte(c.buf[end:], 0)
			nbuf = append(nbuf, c.buf[end:ti+int(end)]...)
		}

		return true
	})

	c.buf = nbuf
}
