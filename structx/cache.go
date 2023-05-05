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
	TickDuration = time.Second / 10
)

type Cache[V any] struct {
	// current timestamp
	ts int64

	// pairs count in duration
	count int64

	// call when key-value expired
	onExpired func(string, V, int64)

	// data based on ZSet
	data *ZSet[string, int64, V]

	enabledMu bool
	mu        sync.RWMutex
}

// NewCache
func NewCache[V any]() *Cache[V] {
	cache := &Cache[V]{
		ts:        time.Now().UnixNano(),
		data:      NewZSet[string, int64, V](),
		enabledMu: true,
	}
	go cache.eviction()

	return cache
}

// NewUnsafeCache
func NewUnsafeCache[V any]() *Cache[V] {
	cache := &Cache[V]{
		ts:        time.Now().UnixNano(),
		data:      NewZSet[string, int64, V](),
		enabledMu: false,
	}
	go cache.eviction()

	return cache
}

// Get
func (c *Cache[V]) Get(key string) (val V, ok bool) {
	if c.enabledMu {
		c.mu.RLock()
		defer c.mu.RUnlock()
	}

	v, ttl, ok := c.data.Get(key)
	if ok && (ttl == NoTTL || ttl > c.ts) {
		return v, true
	}
	return
}

// GetPos
func (c *Cache[V]) GetPos(pos uint64) (val V, ok bool) {
	if c.enabledMu {
		c.mu.RLock()
		defer c.mu.RUnlock()
	}

	v, ttl, ok := c.data.GetPos(pos)
	if ok && (ttl == NoTTL || ttl > c.ts) {
		return v, true
	}
	return
}

// GetEX
func (c *Cache[V]) GetEX(key string) (v V, ttl int64, ok bool) {
	if c.enabledMu {
		c.mu.RLock()
		defer c.mu.RUnlock()
	}

	v, ttl, ok = c.data.Get(key)
	if ok && (ttl == NoTTL || ttl > c.ts) {
		return v, ttl, true
	}
	return
}

// Set
func (c *Cache[V]) Set(key string, value V) {
	if c.enabledMu {
		c.mu.Lock()
		defer c.mu.Unlock()
	}

	c.data.Set(key, value)
}

// SetEX
func (c *Cache[V]) SetEX(key string, value V, ttl time.Duration) {
	if c.enabledMu {
		c.mu.Lock()
		defer c.mu.Unlock()
	}

	c.count++
	c.data.SetWithScore(key, c.ts+int64(ttl)+c.count, value)
}

// SetTX
func (c *Cache[V]) SetTX(key string, value V, ts int64) {
	if c.enabledMu {
		c.mu.Lock()
		defer c.mu.Unlock()
	}

	c.count++
	c.data.SetWithScore(key, ts+c.count, value)
}

// Persist
func (c *Cache[V]) Persist(key string) bool {
	if c.enabledMu {
		c.mu.Lock()
		defer c.mu.Unlock()
	}

	item, ok := c.data.data.Get(key)
	if ok {
		c.data.updateScore(item, key, NoTTL)
	}
	return ok
}

// Keys
func (c *Cache[V]) Keys() []string {
	if c.enabledMu {
		c.mu.RLock()
		defer c.mu.RUnlock()
	}

	return c.data.data.Keys()
}

// WithExpired
func (c *Cache[V]) WithExpired(f func(string, V, int64)) {
	if c.enabledMu {
		c.mu.Lock()
		defer c.mu.Unlock()
	}

	c.onExpired = f
}

// Remove
func (c *Cache[V]) Remove(key string) (V, bool) {
	if c.enabledMu {
		c.mu.Lock()
		defer c.mu.Unlock()
	}

	return c.data.Delete(key)
}

// Clear
func (c *Cache[V]) Clear() {
	if c.enabledMu {
		c.mu.Lock()
		defer c.mu.Unlock()
	}

	c.data = NewZSet[string, int64, V]()
}

// Count
func (c *Cache[V]) Count() int {
	if c.enabledMu {
		c.mu.RLock()
		defer c.mu.RUnlock()
	}

	return c.data.Size()
}

// Scheduled update current timestamp and clear expired keys
func (c *Cache[V]) eviction() {
	for c != nil {
		time.Sleep(TickDuration)

		c.mu.Lock()

		// reset
		c.ts = time.Now().UnixNano()
		c.count = 0

		// clear expired keys
		if c.data.Size() > 0 {
			for f := c.data.Iter(); f.Valid(); f.Next() {
				if f.Score() == NoTTL {
					continue
				}
				if f.Score() > c.ts {
					break
				}

				v, ok := c.data.Delete(f.Key())
				if ok {
					if c.onExpired != nil {
						c.onExpired(f.Key(), v, f.Score())
					}
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
