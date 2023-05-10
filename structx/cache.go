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
	probeCount     = 100
	probeSpace     = 3
	expRate        = 0.2
	eliminateMaxMs = 25
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
	return NewCustomCache[V](expRate)
}

// NewCustomCache
func NewCustomCache[V any](expRate float64) *Cache[V] {
	c := &Cache[V]{
		ts:   time.Now().UnixNano(),
		data: NewMap[string, *cacheItem[V]](),
	}
	go c.eliminate(expRate)
	return c
}

// Get
func (c *Cache[V]) Get(key string) (V, bool) {
	v, _, ok := c.GetTX(key)
	return v, ok
}

// GetTX
func (c *Cache[V]) GetTX(key string) (v V, ttl int64, ok bool) {
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
	c.SetTX(key, val, noTTL)
}

// SetEX
func (c *Cache[V]) SetEX(key string, val V, ttl time.Duration) {
	c.SetTX(key, val, c.ts+int64(ttl))
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
	return keys
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

// Scan
func (c *Cache[V]) Scan(f func(string, V, int64) bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	c.data.Scan(func(key string, value *cacheItem[V]) bool {
		if value.T > c.ts {
			return f(key, value.V, value.T)
		}
		return true
	})
}

// Count
func (c *Cache[V]) Count() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.data.Len()
}

// eliminate the expired key-value pairs.
func (c *Cache[V]) eliminate(expRate float64) {
	for c != nil {
		time.Sleep(TickInterval)
		c.mu.Lock()

		// update ts
		start := time.Now()
		c.ts = start.UnixNano()

		for {
			// eliminate eval
			var pb, elimi float64

			for i := 0; i < probeCount; i++ {
				k, v, ok := c.data.GetPos(uint64(c.ts) + uint64(i*probeSpace))
				// expired
				if ok && v.T < c.ts {
					elimi++
					c.data.Delete(k)
				}
				pb++
			}

			// update ts
			ts := time.Now()
			c.ts = ts.UnixNano()

			// break if cost over limit or blow expRate
			if ts.Sub(start).Milliseconds() >= eliminateMaxMs || elimi/pb <= expRate {
				break
			}
		}
		c.mu.Unlock()
	}
}
