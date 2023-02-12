package structx

import (
	"sync/atomic"
	"time"

	skiplist "github.com/sean-public/fast-skiplist"
)

const (
	// NoTTL
	NoTTL = -1
)

var (
	// duration of update timestamp and expired keys evictions
	TickDuration = time.Millisecond

	// default expiry time
	DefaultTTL = time.Minute * 10
)

type cacheItem[K string, V any] struct {
	K K
	V V
	T int64 // TTL
}

type Cache[K string, V any] struct {
	// current timestamp
	_now int64

	// pairs count in duration
	_count int64

	// call when key-value expired
	onExpired func(K, V, int64)

	// data
	m *SyncMap[K, *cacheItem[K, V]]

	// ttl skiplist
	skl *skiplist.SkipList
}

func (c *Cache[K, V]) now() int64 {
	return atomic.LoadInt64(&c._now)
}

// NewCache
func NewCache[V any]() *Cache[string, V] {
	cache := &Cache[string, V]{
		// data map
		m: NewSyncMap[*cacheItem[string, V]](),

		// current timstamp
		_now: time.Now().UnixNano(),

		// skiplist
		skl: skiplist.New(),
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
		c.skl.Remove(float64(item.T))
		item.T = c.now() + int64(ttl) + atomic.AddInt64(&c._count, 1)

	} else {
		item = &cacheItem[K, V]{
			key,
			val,
			c.now() + int64(ttl) + atomic.AddInt64(&c._count, 1),
		}
		c.m.Set(key, item)
	}

	// insert
	c.skl.Set(float64(item.T), item)
	return ok
}

// Keys
func (c *Cache[K, V]) Keys() []K {
	return c.m.Keys()
}

// WithExpired
func (c *Cache[K, V]) WithExpired(f func(K, V, int64)) *Cache[K, V] {
	c.onExpired = f
	return c
}

// Remove
func (c *Cache[K, V]) Remove(key K) bool {
	item, ok := c.m.Get(key)
	if ok {
		c.m.Remove(key)
		c.skl.Remove(float64(item.T))
	}
	return ok
}

// Clear
func (c *Cache[K, V]) Clear() {
	c.m.Clear()
	c.skl = skiplist.New()
}

// Count
func (c *Cache[K, V]) Count() int {
	return c.m.Count()
}

// Scheduled update current ts and clear expired keys
func (c *Cache[K, V]) eviction() {
	for c != nil {
		time.Sleep(time.Millisecond)

		// update current ts
		atomic.SwapInt64(&c._now, time.Now().UnixNano())
		atomic.SwapInt64(&c._count, 0)

		// search expired keys
		expiredItems := make([]*cacheItem[K, V], 0)

		for f := c.skl.Front(); f != nil; f = f.Next() {
			if int64(f.Key()) < c.now() {
				val := f.Value().(*cacheItem[K, V])
				expiredItems = append(expiredItems, val)
				continue
			}
			break
		}

		// clear
		for _, item := range expiredItems {
			c.skl.Remove(float64(item.T))
			c.m.Remove(item.K)
			// on expired
			if c.onExpired != nil {
				c.onExpired(item.K, item.V, item.T)
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
