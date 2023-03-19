package test

import (
	"testing"

	"github.com/liyiheng/zset"
	"github.com/xgzlucario/rotom/structx"
)

func getZSet1() *structx.ZSet[int64, float64, any] {
	s := structx.NewZSet[int64, float64, any]()
	for i := 0; i < 10000; i++ {
		s.Incr(int64(i), float64(i))
	}
	return s
}

func getZSet2() *zset.SortedSet {
	s := zset.New()
	for i := 0; i < 10000; i++ {
		s.Set(float64(i), int64(i), nil)
	}
	return s
}

// Add
func Benchmark_ZSetAdd1(b *testing.B) {
	s := structx.NewZSet[int64, float64, any]()
	for i := 0; i < b.N; i++ {
		s.SetWithScore(int64(i)/2, float64(i), "a")
	}
}
func Benchmark_ZSetAdd2(b *testing.B) {
	s := zset.New()
	for i := 0; i < b.N; i++ {
		s.Set(float64(i), int64(i)/2, "a")
	}
}

// Delete
func Benchmark_ZSetDelete1(b *testing.B) {
	s := getZSet1()
	for i := 0; i < b.N; i++ {
		s.Delete(int64(i))
	}
}
func Benchmark_ZSetDelete2(b *testing.B) {
	s := getZSet2()
	for i := 0; i < b.N; i++ {
		s.Delete(int64(i))
	}
}
