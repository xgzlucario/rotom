package structx

import (
	"sync"
	"sync/atomic"
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
	onExpired func(string, V)

	// data based on ZSet
	data *ZSet[string, int64, V]

	sync.RWMutex
}

// NewCache
func NewCache[V any]() *Cache[V] {
	cache := &Cache[V]{
		ts: time.Now().UnixNano(),

		data: NewZSet[string, int64, V](),
	}
	go cache.eviction()

	return cache
}

// Get
func (c *Cache[V]) Get(key string) (val V, ok bool) {
	c.RLock()
	defer c.RUnlock()

	v, ttl, ok := c.data.Get(key)
	// check valid
	if ok && (ttl > c.ts || ttl == NoTTL) {
		return v, true
	}
	return
}

// GetWithTTL
func (c *Cache[V]) GetWithTTL(key string) (v V, ttl int64, ok bool) {
	c.RLock()
	defer c.RUnlock()

	v, ttl, ok = c.data.Get(key)
	// check valid
	if ok && (ttl > c.ts || ttl == NoTTL) {
		return v, ttl, true
	}
	return
}

// Set
func (c *Cache[V]) Set(key string, value V) {
	c.Lock()
	defer c.Unlock()

	c.data.Set(key, value)
}

// SetWithDeadLine
func (c *Cache[V]) SetWithDeadLine(key string, value V, ts int64) {
	c.Lock()
	defer c.Unlock()

	c.data.SetWithScore(key, ts+atomic.AddInt64(&c.count, 1), value)
}

// SetWithTTL
func (c *Cache[V]) SetWithTTL(key string, value V, ttl time.Duration) {
	c.Lock()
	defer c.Unlock()

	c.data.SetWithScore(key, c.ts+int64(ttl)+atomic.AddInt64(&c.count, 1), value)
}

// Persist
func (c *Cache[V]) Persist(key string) bool {
	c.Lock()
	defer c.Unlock()

	item, ok := c.data.data[key]
	if ok {
		c.data.updateScore(item, key, NoTTL)
	}
	return ok
}

// Keys
func (c *Cache[V]) Keys() []string {
	c.RLock()
	defer c.RUnlock()

	return c.data.data.Keys()
}

// WithExpired
func (c *Cache[V]) WithExpired(f func(string, V)) {
	c.Lock()
	defer c.Unlock()

	c.onExpired = f
}

// Remove
func (c *Cache[V]) Remove(key string) (V, bool) {
	c.Lock()
	defer c.Unlock()

	return c.data.Delete(key)
}

// Clear
func (c *Cache[V]) Clear() {
	c.Lock()
	defer c.Unlock()

	c.data = NewZSet[string, int64, V]()
}

// Count
func (c *Cache[V]) Count() int {
	c.RLock()
	defer c.RUnlock()

	return c.data.Size()
}

// Scheduled update current timestamp and clear expired keys
func (c *Cache[V]) eviction() {
	for c != nil {
		time.Sleep(TickDuration)

		// update current timestamp
		c.ts = time.Now().UnixNano()
		// reset count
		atomic.SwapInt64(&c.count, 0)

		c.Lock()

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
					// on expired
					if c.onExpired != nil {
						c.onExpired(f.Key(), v)
					}
				}
			}
		}
		c.Unlock()
	}
}

func (c *Cache[V]) MarshalJSON() ([]byte, error) {
	c.RLock()
	defer c.RUnlock()

	return c.data.MarshalJSON()
}

func (c *Cache[V]) UnmarshalJSON(src []byte) error {
	c.Lock()
	defer c.Unlock()

	return c.data.UnmarshalJSON(src)
}
