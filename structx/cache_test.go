package structx

import (
	"strconv"
	"testing"
	"time"
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

func BenchmarkCacheSetWithTTL(b *testing.B) {
	s := NewCache[int]()
	for i := 0; i < b.N; i++ {
		s.SetWithTTL(strconv.Itoa(i), i, time.Second)
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
