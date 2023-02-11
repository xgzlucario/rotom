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
	// current timestamp update by ticker
	_now int64

	// call when key-value expired
	onExpired func(K, V)

	// data
	m *SyncMap[K, *cacheItem[K, V]]

	// sortedList
	sl *LinkList[*cacheItem[K, V]]
}

func (c *Cache[K, V]) now() int64 {
	return atomic.LoadInt64(&c._now)
}

// NewCache
func NewCache[V any]() *Cache[string, V] {
	cache := &Cache[string, V]{
		// data map
		m: NewSyncMap[*cacheItem[string, V]](),

		// current timestamp
		_now: time.Now().UnixNano(),

		// create sortedList
		sl: NewLinkList(func(a, b *cacheItem[string, V]) bool {
			return a.T < b.T
		}),
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
		c.sl.RemoveFirst(item)

	} else {
		item = &cacheItem[K, V]{key, val, c.now() + int64(ttl)}
		c.m.Set(key, item)
	}

	// update
	c.sl.Insert(item)
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
		c.sl.RemoveFirst(v)
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

// Scheduled update current timestamp and clear expired keys
func (c *Cache[K, V]) eviction() {
	for c != nil {
		time.Sleep(TickDuration)

		// update current timestamp
		atomic.SwapInt64(&c._now, time.Now().UnixNano())

		// clear expired keys
		for p := c.sl.head; p != nil; {
			if p.val.T < c.now() {
				// remove
				c.m.Remove(p.val.K)
				c.sl.Remove(p)

				if c.onExpired != nil {
					c.onExpired(p.val.K, p.val.V)
				}

			} else {
				break
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
