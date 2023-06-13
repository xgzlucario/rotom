package test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	"github.com/xgzlucario/rotom/structx"
)

func TestCache(t *testing.T) {
	t.Parallel()

	const noTTL int64 = 0
	cache := structx.NewCache[int]()

	// Test Set and Get
	cache.Set("key1", 1)
	value, ok := cache.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, 1, value)

	// Test SetEX and Get
	cache.SetEx("key2", 2, time.Millisecond*50)
	value, ok = cache.Get("key2")
	assert.True(t, ok)
	assert.Equal(t, 2, value)

	// Test expiration of key2
	time.Sleep(time.Millisecond * 60)
	_, ok = cache.Get("key2")
	assert.False(t, ok)

	// Test GetTX
	cache.Set("key3", 3)
	value, ttl, ok := cache.GetTX("key3")
	assert.True(t, ok)
	assert.Equal(t, 3, value)
	assert.Equal(t, noTTL, ttl)

	// Test SetEX
	cache.SetEx("key4", 4, time.Millisecond*100)
	value, ok = cache.Get("key4")
	assert.True(t, ok)
	assert.Equal(t, 4, value)

	// Test Persist
	cache.Persist("key3")
	value, ttl, ok = cache.GetTX("key3")
	assert.True(t, ok)
	assert.Equal(t, 3, value)
	assert.Equal(t, noTTL, ttl)

	// Test Keys
	keys := cache.Keys()
	fmt.Println(cache.Keys())
	assert.Contains(t, keys, "key1")
	assert.Contains(t, keys, "key3")
	assert.Contains(t, keys, "key4")

	// Test Remove
	value, ok = cache.Remove("key1")
	assert.True(t, ok)
	assert.Equal(t, 1, value)

	// Test Clear
	cache.Clear()
	_, ok = cache.Get("key3")
	assert.False(t, ok)

	// Test Scan
	cache.Set("key5", 5)
	cache.Set("key6", 6)
	cache.Set("key7", 7)

	cache.Scan(func(key string, value int, ttl int64) bool {
		assert.True(t, value >= 5 && value <= 7)
		return true
	})

	// Test Count
	assert.Equal(t, 3, cache.Count())
}

func TestBigCache(t *testing.T) {
	cache := structx.NewBigCache()
	valid := map[string][]byte{}

	for i := 0; i < 100000; i++ {
		p := gofakeit.Phone()

		valid[p] = []byte(p)
		cache.Set(p, []byte(p))
	}

	for k, v := range valid {
		value, ok := cache.Get(k)
		assert.True(t, ok)
		assert.Equal(t, v, value)
	}
}

func BenchmarkCache(b *testing.B) {
	c := structx.NewCache[int]()

	b.Run("CacheTest", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if i%3 == 1 {
				c.Remove(strconv.Itoa(i - 1))
			} else {
				c.Set(strconv.Itoa(i), i)
			}
		}
	})

	m := structx.NewMap[string, int]()

	b.Run("HashMapTest", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if i%3 == 1 {
				m.Delete(strconv.Itoa(i - 1))
			} else {
				m.Set(strconv.Itoa(i), i)
			}
		}
	})

	m1 := map[string]int{}

	b.Run("StdMapTest", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if i%3 == 1 {
				delete(m1, strconv.Itoa(i-1))
			} else {
				m1[strconv.Itoa(i)] = i
			}
		}
	})
}

func BenchmarkNowCache(b *testing.B) {
	m := structx.NewCache[[]byte]()
	b.Run("CacheSet", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			s := strconv.Itoa(i)
			m.Set(s, []byte(s))
		}
	})
}

func BenchmarkBigCache(b *testing.B) {
	m := structx.NewBigCache()
	b.Run("BigCacheSet", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			s := strconv.Itoa(i)
			m.Set(s, []byte(s))
		}
	})
}
