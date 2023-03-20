package test

import (
	"testing"

	"github.com/xgzlucario/rotom/structx"
)

func BenchmarkList_LPush(b *testing.B) {
	lst := structx.NewList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lst.LPush(0)
	}
}

func BenchmarkList_RPush(b *testing.B) {
	lst := structx.NewList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lst.RPush(11)
	}
}

func BenchmarkList_Insert(b *testing.B) {
	lst := structx.NewList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lst.Insert(5, 11)
	}
}

func BenchmarkList_LPop(b *testing.B) {
	lst := structx.NewList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lst.LPop()
	}
}

func BenchmarkList_RPop(b *testing.B) {
	lst := structx.NewList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lst.RPop()
	}
}

func BenchmarkList_RemoveFirst(b *testing.B) {
	lst := structx.NewList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lst.RemoveFirst(5)
	}
}

func BenchmarkList_RemoveIndex(b *testing.B) {
	lst := structx.NewList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lst.RemoveIndex(5)
	}
}

func BenchmarkList_Max(b *testing.B) {
	lst := structx.NewList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lst.Max(func(t1, t2 int) bool {
			return t1 < t2
		})
	}
}

func BenchmarkList_Min(b *testing.B) {
	lst := structx.NewList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lst.Min(func(t1, t2 int) bool {
			return t1 < t2
		})
	}
}

func BenchmarkList_Sort(b *testing.B) {
	lst := structx.NewList(10, 9, 8, 7, 6, 5, 4, 3, 2, 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lst.Sort(func(t1, t2 int) bool {
			return t1 < t2
		})
	}
}

func BenchmarkList_IsSorted(b *testing.B) {
	lst := structx.NewList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lst.IsSorted(func(t1, t2 int) bool {
			return t1 < t2
		})
	}
}