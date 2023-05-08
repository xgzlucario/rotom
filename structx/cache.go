package structx

import (
	"math"
	"sync"
	"time"
)

const (
	// noTTL means the expiration time is infinite
	noTTL = math.MaxInt64

	// probe config with elimination strategy
	probeCount = 100
	probeLimit = 10000
	probeSpace = 3
)

var (
	// Interval of eliminate expired items and update timestamp
	TickInterval = time.Millisecond * 10
)

type Cache[V any] struct {
	// current timestamp
	ts int64

	// based on Hashmap
	data Map[string, *cacheItem[V]]

	mu sync.RWMutex
}

type cacheItem[V any] struct {
	T int64
	V V
}

// NewCache
func NewCache[V any]() *Cache[V] {
	return NewCustomCache[V](probeCount, probeLimit, probeSpace)
}

// NewCustomCache
func NewCustomCache[V any](pbCount int, pbLimit, pbSpace uint64) *Cache[V] {
	c := &Cache[V]{
		ts:   time.Now().UnixNano(),
		data: NewMap[string, *cacheItem[V]](),
	}
	go c.eliminate(pbCount, pbLimit, pbSpace)
	return c
}

// Get
func (c *Cache[V]) Get(key string) (val V, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	n, ok := c.data.Get(key)
	if ok && n.T > c.ts {
		return n.V, true
	}
	return
}

// GetEX
func (c *Cache[V]) GetEX(key string) (v V, ttl int64, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	n, ok := c.data.Get(key)
	if ok && n.T > c.ts {
		return n.V, n.T, true
	}
	return
}

// Set
func (c *Cache[V]) Set(key string, val V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data.Set(key, &cacheItem[V]{T: noTTL, V: val})
}

// SetEX
func (c *Cache[V]) SetEX(key string, val V, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data.Set(key, &cacheItem[V]{T: c.ts + int64(ttl), V: val})
}

// SetTX
func (c *Cache[V]) SetTX(key string, val V, ts int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data.Set(key, &cacheItem[V]{T: ts, V: val})
}

// Persist
func (c *Cache[V]) Persist(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	n, ok := c.data.Get(key)
	if ok {
		n.T = noTTL
	}
	return ok
}

// Keys
func (c *Cache[V]) Keys() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	keys := make([]string, 0, c.data.Len())
	c.data.Scan(func(k string, v *cacheItem[V]) bool {
		if v.T > c.ts {
			keys = append(keys, k)
		}
		return true
	})
	return nil
}

// Remove
func (c *Cache[V]) Remove(key string) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	n, ok := c.data.Delete(key)
	return n.V, ok
}

// Clear
func (c *Cache[V]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data = NewMap[string, *cacheItem[V]]()
}

// Count
func (c *Cache[V]) Count() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.data.Len()
}

// eliminate the expired key-value pairs.
// This elimination strategy can to keep the elimination rate blow 10%.
func (c *Cache[V]) eliminate(pbCount int, pbLimit, pbSpace uint64) {
	for c != nil {
		time.Sleep(TickInterval)

		c.mu.Lock()
		// reset
		c.ts = time.Now().UnixNano()
		offset := uint64(0)

		// probe and eliminate
		for i := 0; i < pbCount; i++ {
			offset += pbSpace
			if offset/3 >= pbLimit {
				break
			}

			k, v, ok := c.data.GetPos(uint64(c.ts) + offset)
			// expired
			if ok && v.T < c.ts {
				i = 0
				c.data.Delete(k)
			}
		}
		c.mu.Unlock()
	}
}

func (c *Cache[V]) MarshalJSON() ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.data.MarshalJSON()
}

func (c *Cache[V]) UnmarshalJSON(src []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.data.UnmarshalJSON(src)
}
