package structx

import (
	"strconv"
	"testing"
	"time"
)

func getCache() *Cache[string, int] {
	s := NewCache[int]()
	for i := 0; i < 1000000; i++ {
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
	s := getCache()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Get(strconv.Itoa(i))
	}
}

func BenchmarkCacheRemove(b *testing.B) {
	s := getCache()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Remove(strconv.Itoa(i))
	}
}

func BenchmarkCacheCount(b *testing.B) {
	s := getCache()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Count()
	}
}
