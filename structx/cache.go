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

	pool sync.Pool

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
		ts: time.Now().UnixNano(),
		pool: sync.Pool{New: func() any {
			return new(cacheItem[V])
		}},
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
	c.SetTx(key, val, noTTL)
}

// SetEx
func (c *Cache[V]) SetEx(key string, val V, ttl time.Duration) {
	c.SetTx(key, val, c.ts+int64(ttl))
}

// SetTx
func (c *Cache[V]) SetTx(key string, val V, ts int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	obj := c.pool.Get().(*cacheItem[V])
	obj.T = ts
	obj.V = val
	c.data.Set(key, obj)
}

// Remove
func (c *Cache[V]) Remove(key string) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.delete(key)
}

// delete
func (c *Cache[V]) delete(key string) (V, bool) {
	n, ok := c.data.Delete(key)
	defer func() {
		var v V
		n.V = v
		c.pool.Put(n)
	}()
	return n.V, ok
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

// Size
func (c *Cache[V]) Size() int {
	return c.data.Len()
}

// Keys
func (c *Cache[V]) Keys() []string {
	keys := make([]string, 0)

	c.Scan(func(k string, _ V, ts int64) bool {
		if ts > c.ts {
			keys = append(keys, k)
		}
		return true
	})

	return keys
}

// Count
func (c *Cache[V]) Count() (sum int) {
	c.Scan(func(_ string, _ V, ts int64) bool {
		if ts > c.ts {
			sum++
		}
		return true
	})
	return
}

// Clear
func (c *Cache[V]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data = NewMap[string, *cacheItem[V]]()
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
					c.delete(k)
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
