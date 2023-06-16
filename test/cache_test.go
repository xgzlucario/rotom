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
	ttl := map[string]int64{}

	for i := 0; i < 100000; i++ {
		p := gofakeit.Phone()

		ts := time.Now().UnixNano() << 1

		valid[p] = []byte(p)
		ttl[p] = ts

		cache.SetTx(p, []byte(p), ts)
	}

	for k, v := range valid {
		value, ts, ok := cache.GetTx(k)
		assert.True(t, ok)
		assert.Equal(t, v, value)
		assert.Equal(t, ttl[k], ts)
	}
}

func BenchmarkCache(b *testing.B) {
	c := structx.NewCache[int]()
	b.Run("CacheSet", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			c.Set(strconv.Itoa(i), i)
		}
	})

	m := structx.NewMap[string, int]()
	b.Run("HashMapSet", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Set(strconv.Itoa(i), i)
		}
	})

	m1 := map[string]int{}
	b.Run("StdMapSet", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m1[strconv.Itoa(i)] = i
		}
	})

	bc := structx.NewBigCache()
	b.Run("BigCacheSet", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bc.Set(strconv.Itoa(i), []byte{1, 2, 3, 4, 5, 6, 7, 8})
		}
	})
}
