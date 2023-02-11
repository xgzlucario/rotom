package structx

import (
	"sync/atomic"
	"time"
)

var (
	// duration of update timestamp and expired keys evictions
	TickDuration = time.Millisecond

	// default expiry time
	DefaultTTL = time.Minute * 10

	NoTTL int64 = -1
)

type cacheItem[K string, V any] struct {
	K K
	V V
	T int64 // TTL
}

type Cache[K string, V any] struct {
	// current ts
	_now int64

	// call when key-value expired
	onExpired func(K, V)

	// data
	m *SyncMap[K, *cacheItem[K, V]]

	// List
	ls *List[*cacheItem[K, V]]
}

func (c *Cache[K, V]) now() int64 {
	return atomic.LoadInt64(&c._now)
}

// NewCache
func NewCache[V any]() *Cache[string, V] {
	cache := &Cache[string, V]{
		// data map
		m: NewSyncMap[*cacheItem[string, V]](),

		// current ts
		_now: time.Now().UnixNano(),

		// create List
		ls: NewList[*cacheItem[string, V]](),
	}
	go cache.eviction()

	return cache
}

// IsEmpty
func (c *Cache[K, V]) IsEmpty() bool {
	return c.m.IsEmpty()
}

// Get
func (c *Cache[K, V]) Get(key K) (val V, ok bool) {
	item, ok := c.m.Get(key)
	if !ok {
		return
	}
	// check valid
	if item.T > c.now() || item.T == NoTTL {
		return item.V, true
	}
	return
}

// GetWithTTL
func (c *Cache[K, V]) GetWithTTL(key K) (v V, ttl int64, ok bool) {
	item, ok := c.m.Get(key)
	if !ok {
		return
	}
	// check valid
	if item.T > c.now() || item.T == NoTTL {
		return item.V, item.T, true
	}
	return
}

// Set
func (c *Cache[K, V]) Set(key K, value V) {
	// if exist
	item, ok := c.m.Get(key)
	if ok {
		item.T = NoTTL
		item.V = value

	} else {
		item := &cacheItem[K, V]{key, value, NoTTL}
		c.m.Set(key, item)
	}
}

// SetWithTTL
func (c *Cache[K, V]) SetWithTTL(key K, val V, ttl time.Duration) bool {
	item, ok := c.m.Get(key)
	// exist
	if ok {
		item.V = val
		item.T = c.now() + int64(ttl)
		c.ls.RemoveFirst(item)

	} else {
		item = &cacheItem[K, V]{key, val, c.now() + int64(ttl)}
		c.m.Set(key, item)
	}

	// update
	c.ls.RPush(item)
	return ok
}

// Keys
func (c *Cache[K, V]) Keys() []K {
	return c.m.Keys()
}

// OnExpired
func (c *Cache[K, V]) OnExpired(f func(K, V)) *Cache[K, V] {
	c.onExpired = f
	return c
}

// Remove
func (c *Cache[K, V]) Remove(key K) bool {
	v, ok := c.m.Get(key)
	if ok {
		c.m.Remove(key)
		c.ls.RemoveFirst(v)
	}
	return ok
}

// Clear
func (c *Cache[K, V]) Clear() {
	c.m.Clear()
}

// Count
func (c *Cache[K, V]) Count() int {
	return c.m.Count()
}

// Scheduled update current ts and clear expired keys
func (c *Cache[K, V]) eviction() {
	for c != nil {
		time.Sleep(TickDuration)

		// update current ts
		atomic.SwapInt64(&c._now, time.Now().UnixNano())

		// clear expired keys
		c.ls.Sort(func(t1, t2 *cacheItem[K, V]) bool {
			return t1.T < t2.T
		})

		expiredKeys := make([]*cacheItem[K, V], 0)

		c.ls.Range(func(item *cacheItem[K, V]) bool {
			if item.T < c.now() {
				expiredKeys = append(expiredKeys, item)
				return false
			}
			return true
		})

		for _, e := range expiredKeys {
			// remove
			c.m.Remove(e.K)
			c.ls.RemoveFirst(e)

			if c.onExpired != nil {
				c.onExpired(e.K, e.V)
			}
		}
	}
}

// Print
func (c *Cache[K, V]) Print() {
	c.m.Print()
}

func (c *Cache[K, V]) MarshalJSON() ([]byte, error) {
	return c.m.MarshalJSON()
}

func (c *Cache[K, V]) UnmarshalJSON(src []byte) error {
	return c.m.UnmarshalJSON(src)
}
