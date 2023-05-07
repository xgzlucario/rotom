package structx

import (
	"math"
	"sync"
	"time"
)

const (
	noTTL = math.MaxInt64

	probeCount = 100

	probeSpace = 3
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
	go cache.eliminate()
	return cache
}

// Get
func (c *Cache[V]) Get(key string) (val V, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	n, ok := c.data.Get(key)
	if ok && n.T > c.ts {
		return n.V, true
	}
	return
}

// GetEX
func (c *Cache[V]) GetEX(key string) (v V, ttl int64, ok bool) {
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
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data.Set(key, &cacheItem[V]{T: noTTL, V: val})
}

// SetEX
func (c *Cache[V]) SetEX(key string, val V, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data.Set(key, &cacheItem[V]{T: c.ts + int64(ttl), V: val})
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

	return nil
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

// eval for DEBUG
func (c *Cache[V]) eval() (start time.Time, expired, total int) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	start = time.Now()

	c.data.Scan(func(_ string, value *cacheItem[V]) bool {
		if value.T < c.ts {
			expired++
		}
		return true
	})

	return start, expired, c.data.Len()
}

// eliminate the expired key-value pairs.
// This elimination strategy has been tested to keep the elimination rate below 15%.
func (c *Cache[V]) eliminate() {
	for c != nil {
		time.Sleep(TickDuration)
		// start := time.Now()

		c.mu.Lock()

		// reset
		c.ts = time.Now().UnixNano()
		offset := uint64(0)

		for i := 0; i < probeCount; i++ {
			offset += probeSpace
			k, v, ok := c.data.GetPos(uint64(c.ts) + offset)
			// expired
			if ok && v.T < c.ts {
				i = 0
				c.data.Delete(k)
			}
		}
		c.mu.Unlock()

		// end, a, b := c.eval()
		// eval: {expiredCount} / {totalCount} -> {expiredRate} cost: {time}
		// fmt.Printf("eval: %d / %d -> %.2f%% cost: %v\n", a, b, float64(a)/float64(b)*100, end.Sub(start))
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
