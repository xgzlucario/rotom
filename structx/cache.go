package structx

import (
	"sync/atomic"
	"time"

	"github.com/xgzlucario/rotom/base"
)

const (
	// NoTTL
	NoTTL = -1
)

var (
	// duration of update timestamp and expired keys evictions
	TickDuration = time.Millisecond
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
	onExpired func(K, V)

	// data map
	m *SyncMap[K, *cacheItem[K, V]]

	// expired key-value pairs rbtree
	rb *RBTree[int64, *cacheItem[K, V]]
}

func (c *Cache[K, V]) now() int64 {
	return atomic.LoadInt64(&c._now)
}

// NewCache
func NewCache[V any]() *Cache[string, V] {
	cache := &Cache[string, V]{
		_now: time.Now().UnixNano(),

		m: NewSyncMap[*cacheItem[string, V]](),

		rb: NewRBTree[int64, *cacheItem[string, V]](),
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
		item = &cacheItem[K, V]{key, value, NoTTL}
		c.m.Set(key, item)
	}
}

// SetWithTTL
func (c *Cache[K, V]) SetWithTTL(key K, val V, ttl time.Duration) bool {
	item, ok := c.m.Get(key)
	// exist
	if ok {
		item.V = val
		c.rb.Delete(item.T)
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
	c.rb.Insert(item.T, item)
	return ok
}

// Persist
func (c *Cache[K, V]) Persist(key K) bool {
	item, ok := c.m.Get(key)
	if !ok {
		return false
	}
	// persist
	if item.T != NoTTL {
		c.rb.Delete(item.T)
		item.T = NoTTL
	}
	return true
}

// Keys
func (c *Cache[K, V]) Keys() []K {
	return c.m.Keys()
}

// WithExpired
func (c *Cache[K, V]) WithExpired(f func(K, V)) *Cache[K, V] {
	c.onExpired = f
	return c
}

// Remove
func (c *Cache[K, V]) Remove(key K) bool {
	item, ok := c.m.Get(key)
	if ok {
		c.m.Remove(key)
		c.rb.Delete(item.T)
	}
	return ok
}

// Clear
func (c *Cache[K, V]) Clear() {
	c.m.Clear()
	c.rb = NewRBTree[int64, *cacheItem[K, V]]()
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
		// reset count
		atomic.SwapInt64(&c._count, 0)

		// clear expired keys
		for !c.rb.Empty() {
			f := c.rb.Iterator()
			if f.Key > c.now() {
				break
			}

			c.rb.Delete(f.Key)
			c.m.Remove(f.Value.K)
			// on expired
			if c.onExpired != nil {
				c.onExpired(f.Value.K, f.Value.V)
			}
		}
	}
}

func (c *Cache[K, V]) MarshalJSON() ([]byte, error) {
	return base.MarshalJSON(c.m.Items())
}

func (c *Cache[K, V]) UnmarshalJSON(src []byte) error {
	c.Clear()

	// init map
	var tmp map[K]*cacheItem[K, V]
	if err := base.UnmarshalJSON(src, &tmp); err != nil {
		return err
	}
	c.m.MSet(tmp)

	// init tree
	c.rb = NewRBTree[int64, *cacheItem[K, V]]()
	for _, item := range tmp {
		c.rb.Insert(item.T, item)
	}
	return nil
}
