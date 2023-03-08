package structx

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	// NoTTL
	NoTTL = -1
)

var (
	// duration of update timestamp and expired keys evictions
	TickDuration = time.Millisecond * 10
)

type cacheItem[V any] struct {
	V V
	T int64 // TTL
}

type Cache[V any] struct {
	// current timestamp
	ts int64

	// pairs count in duration
	count int64

	// call when key-value expired
	onExpired func(string, V)

	// data trie
	data *Trie[*cacheItem[V]]

	// expired key-value pairs
	ttl *RBTree[int64, string]

	mu sync.RWMutex
}

// NewCache
func NewCache[V any]() *Cache[V] {
	cache := &Cache[V]{
		ts: time.Now().UnixNano(),

		data: NewTrie[*cacheItem[V]](),

		ttl: NewRBTree[int64, string](),
	}
	go cache.eviction()

	return cache
}

// Get
func (c *Cache[V]) Get(key string) (val V, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, ok := c.data.Get(key)
	if !ok {
		return
	}
	// check valid
	if item.T > c.ts || item.T == NoTTL {
		return item.V, true
	}
	return
}

// GetWithTTL
func (c *Cache[V]) GetWithTTL(key string) (v V, ttl int64, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, ok := c.data.Get(key)
	if !ok {
		return
	}
	// check valid
	if item.T > c.ts || item.T == NoTTL {
		return item.V, item.T, true
	}
	return
}

// Set
func (c *Cache[V]) Set(key string, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// if exist
	item, ok := c.data.Get(key)
	if ok {
		item.T = NoTTL
		item.V = value

	} else {
		item = &cacheItem[V]{value, NoTTL}
		c.data.Put(key, item)
	}
}

// SetWithTTL
func (c *Cache[V]) SetWithTTL(key string, val V, ttl time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	item, ok := c.data.Get(key)
	// exist
	if ok {
		item.V = val
		c.ttl.Delete(item.T)
		item.T = c.ts + int64(ttl) + atomic.AddInt64(&c.count, 1)

	} else {
		item = &cacheItem[V]{
			val,
			c.ts + int64(ttl) + atomic.AddInt64(&c.count, 1),
		}
		c.data.Put(key, item)
	}

	// insert
	c.ttl.Insert(item.T, key)
	return ok
}

// Persist
func (c *Cache[V]) Persist(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	item, ok := c.data.Get(key)
	if !ok {
		return false
	}
	// persist
	if item.T != NoTTL {
		c.ttl.Delete(item.T)
		item.T = NoTTL
	}
	return true
}

// Keys
func (c *Cache[V]) Keys() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.data.Keys()
}

// KeysWithPrefix
func (c *Cache[V]) KeysWithPrefix(prefix string) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.data.KeysWithPrefix(prefix)
}

// WithExpired
func (c *Cache[V]) WithExpired(f func(string, V)) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.onExpired = f
}

// Remove
func (c *Cache[V]) Remove(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	item, ok := c.data.Get(key)
	if ok {
		c.data.Remove(key)
		c.ttl.Delete(item.T)
	}
	return ok
}

// Clear
func (c *Cache[V]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data = NewTrie[*cacheItem[V]]()
	c.ttl = NewRBTree[int64, string]()
}

// Count
func (c *Cache[V]) Count() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

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

		c.mu.Lock()

		// clear expired keys
		for !c.ttl.Empty() {
			f := c.ttl.Iterator()
			if f.Key > c.ts {
				break
			}

			c.ttl.Delete(f.Key)
			item, ok := c.data.Get(f.Value)
			if ok {
				c.data.Remove(f.Value)
				// on expired
				if c.onExpired != nil {
					c.onExpired(f.Value, item.V)
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

	if err := c.data.UnmarshalJSON(src); err != nil {
		return err
	}

	// init
	keys, vals := c.data.collectAll(c.data.root, nil, nil, nil)
	for i, val := range vals {
		if val.T != NoTTL {
			c.ttl.Insert(val.T, keys[i])
		}
	}
	return nil
}
