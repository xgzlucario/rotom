package structx

import (
	"sync"
	"time"
)

const (
	// NoTTL
	NoTTL = 0
)

var (
	// duration of update timestamp and expired keys evictions
	TickDuration = time.Millisecond * 10
)

type cacheItem[V any] struct {
	T int64
	V V
}

type Cache[V any] struct {
	// current timestamp
	ts int64

	// call when key-value expired
	onExpired func(string, V, int64)

	// data based on Map
	data Map[string, *cacheItem[V]]

	mu sync.RWMutex
}

// NewCache
func NewCache[V any]() *Cache[V] {
	cache := &Cache[V]{
		ts:   time.Now().UnixNano(),
		data: NewMap[string, *cacheItem[V]](),
	}
	go cache.eviction()

	return cache
}

// Get
func (c *Cache[V]) Get(key string) (val V, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	n, ok := c.data.Get(key)
	if ok && (n.T == NoTTL || n.T > c.ts) {
		return n.V, true
	}
	return
}

// GetEX
func (c *Cache[V]) GetEX(key string) (v V, ttl int64, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	n, ok := c.data.Get(key)
	if ok && (n.T == NoTTL || n.T > c.ts) {
		return n.V, n.T, true
	}
	return
}

// Set
func (c *Cache[V]) Set(key string, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data.Set(key, &cacheItem[V]{V: value})
}

// SetEX
func (c *Cache[V]) SetEX(key string, value V, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data.Set(key, &cacheItem[V]{T: c.ts + int64(ttl), V: value})
}

// SetTX
func (c *Cache[V]) SetTX(key string, value V, ts int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data.Set(key, &cacheItem[V]{T: ts, V: value})
}

// Persist
func (c *Cache[V]) Persist(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	n, ok := c.data.Get(key)
	if ok {
		n.T = NoTTL
	}
	return ok
}

// Keys
func (c *Cache[V]) Keys() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return nil
}

// WithExpired
func (c *Cache[V]) WithExpired(f func(string, V, int64)) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.onExpired = f
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

// Scheduled update current timestamp and clear expired keys
func (c *Cache[V]) eviction() {
	for c != nil {
		time.Sleep(TickDuration)

		c.mu.Lock()

		// reset
		c.ts = time.Now().UnixNano()
		count := uint64(0)

		for flag := 0; flag < 30; flag++ {
			count++
			k, v, ok := c.data.GetPos(uint64(c.ts) + count)
			if ok {
				if v.T > c.ts || v.T == NoTTL {
					continue
				}

				// expired
				flag = 0
				c.data.Delete(k)
				if c.onExpired != nil {
					c.onExpired(k, v.V, v.T)
				}
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
