package structx

import (
	"strconv"
	"testing"
)

var defaultCache = getCache()

func getCache() *Cache[string, int] {
	s := NewCache[int]()
	for i := 0; i < million; i++ {
		s.Set(strconv.Itoa(i), i)
	}
	return s
}

func BenchmarkCacheSet(b *testing.B) {
	s := NewCache[int]()
	for i := 0; i < b.N; i++ {
		s.Set(strconv.Itoa(i), i)
	}
}

func BenchmarkCacheMSet(b *testing.B) {
	s := NewCache[int]()
	for i := 0; i < b.N; i++ {
		s.MSet(map[string]int{
			strconv.Itoa(i):     i,
			strconv.Itoa(i + 1): i,
			strconv.Itoa(i + 2): i,
			strconv.Itoa(i + 3): i,
			strconv.Itoa(i + 4): i,
		})
	}
}

func BenchmarkCacheGet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		defaultCache.Get(strconv.Itoa(i))
	}
}

func BenchmarkCacheRemove(b *testing.B) {
	s := getCache()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Remove(strconv.Itoa(i))
	}
}

func BenchmarkCacheRange(b *testing.B) {
	for i := 0; i < b.N; i++ {
		defaultCache.Range(func(key string, value int) bool {
			return false
		})
	}
}
