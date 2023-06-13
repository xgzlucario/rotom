package structx

import (
	"encoding/binary"
	"sync"
)

const (
	startBitsize  = 32
	offsetBitsize = 31
	offsetMask    = 0xffffffff
	ttlBitsize    = 8
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
	return Idx(start<<startBitsize | offset<<1 | hasTTL&1)
}

type BigCache struct {
	buf []byte
	idx Map[string, Idx]
	sync.RWMutex
}

// NewBigCache returns a new BigCache.
func NewBigCache() *BigCache {
	return &BigCache{
		buf: make([]byte, 0, 1024),
		idx: NewMap[string, Idx](),
	}
}

// Set
func (c *BigCache) Set(key string, value []byte) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.idx.Get(key); ok {
		return
	}

	c.idx.Set(key, newIdx(uint64(len(c.buf)), uint64(len(value)), 0))
	c.buf = append(c.buf, value...)
}

// SetWithTTL
func (c *BigCache) SetWithTTL(key string, value []byte, ts int64) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.idx.Get(key); ok {
		return
	}

	c.idx.Set(key, newIdx(uint64(len(c.buf)), uint64(len(value)), 1))
	c.buf = append(c.buf, value...)

	ttlBuf := make([]byte, ttlBitsize)
	binary.PutVarint(ttlBuf, ts)

	c.buf = append(c.buf, ttlBuf...)
}

// Get
func (c *BigCache) Get(key string) ([]byte, bool) {
	c.RLock()
	defer c.RUnlock()

	if idx, ok := c.idx.Get(key); ok {
		return c.buf[idx.start() : idx.start()+idx.offset()], true
	}
	return nil, false
}

// GetWithTTL
func (c *BigCache) GetWithTTL(key string) ([]byte, int64, bool) {
	c.RLock()
	defer c.RUnlock()

	if idx, ok := c.idx.Get(key); ok {
		// has ttl
		if idx.hasTTL() {
			i := idx.start() + idx.offset()
			ttl, _ := binary.Varint(c.buf[i : i+ttlBitsize])
			return c.buf[idx.start():i], ttl, true
		}
		return c.buf[idx.start() : idx.start()+idx.offset()], -1, true
	}
	return nil, 0, false
}

// Remove
func (c *BigCache) Remove(key string) bool {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.idx.Get(key); ok {
		c.idx.Delete(key)
		return true
	}
	return false
}

// Len
func (c *BigCache) Len() int {
	c.RLock()
	defer c.RUnlock()

	return c.idx.Len()
}
