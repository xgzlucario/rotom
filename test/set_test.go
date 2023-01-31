package test

import (
	"testing"

	"github.com/xgzlucario/rotom/structx"
)

func getListSet(n int) *structx.LSet[int] {
	s := structx.NewLSet[int]()
	for i := 0; i < n; i++ {
		if i%2 == 0 {
			s.Add(i)
		} else {
			s.Add(n - i)
		}
	}
	return s
}

func BenchmarkAdd(b *testing.B) {
	s := structx.NewLSet[int]()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Add(i)
	}
}

func BenchmarkExist(b *testing.B) {
	s := getListSet(million)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Exist(1622)
	}
}

func BenchmarkRemove(b *testing.B) {
	s := getListSet(million)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Remove(i)
	}
}

func BenchmarkRandomPop(b *testing.B) {
	s := getListSet(million)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.RandomPop()
	}
}

func BenchmarkEqual(b *testing.B) {
	s := getListSet(thousand)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Equal(s)
	}
}

func BenchmarkUnion(b *testing.B) {
	s := getListSet(thousand)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Union(s)
	}
}

func BenchmarkIntersect(b *testing.B) {
	s := getListSet(thousand)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Intersect(s)
	}
}

func BenchmarkDiff(b *testing.B) {
	s := getListSet(thousand)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Difference(s)
	}
}

func BenchmarkRange(b *testing.B) {
	s := getListSet(thousand)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Range(func(v int) bool {
			return false
		})
	}
}

func BenchmarkIsSubSet(b *testing.B) {
	s := getListSet(thousand)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.IsSubSet(s)
	}
}
