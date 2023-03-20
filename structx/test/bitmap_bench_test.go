package test

import (
	"testing"

	"github.com/xgzlucario/rotom/structx"
)

func BenchmarkBitMapAdd(b *testing.B) {
	bm := structx.NewBitMap()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bm.Add(uint32(i))
	}
}

func BenchmarkBitMapRemove(b *testing.B) {
	bm := structx.NewBitMap()
	// Add all elements first
	for i := 0; i < b.N; i++ {
		bm.Add(uint32(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bm.Remove(uint32(i))
	}
}

func BenchmarkBitMapContains(b *testing.B) {
	bm := structx.NewBitMap()
	// Add all elements first
	for i := 0; i < b.N; i++ {
		bm.Add(uint32(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bm.Contains(uint32(i))
	}
}

func BenchmarkBitMapUnion(b *testing.B) {
	bm1 := structx.NewBitMap()
	// Add half of the elements to bm1
	for i := 0; i < b.N/2; i++ {
		bm1.Add(uint32(i))
	}
	bm2 := structx.NewBitMap()
	// Add the other half of the elements to bm2
	for i := b.N / 2; i < b.N; i++ {
		bm2.Add(uint32(i))
	}
	b.ResetTimer()
	bm1.Union(bm2)
}

func BenchmarkBitMapIntersect(b *testing.B) {
	bm1 := structx.NewBitMap()
	// Add all elements to bm1
	for i := 0; i < b.N; i++ {
		bm1.Add(uint32(i))
	}
	bm2 := structx.NewBitMap()
	// Add even elements to bm2
	for i := 0; i < b.N; i += 2 {
		bm2.Add(uint32(i))
	}
	b.ResetTimer()
	bm1.Intersect(bm2)
}

func BenchmarkBitMapDifference(b *testing.B) {
	bm1 := structx.NewBitMap()
	// Add all elements to bm1
	for i := 0; i < b.N; i++ {
		bm1.Add(uint32(i))
	}
	bm2 := structx.NewBitMap()
	// Add even elements to bm2
	for i := 0; i < b.N; i += 2 {
		bm2.Add(uint32(i))
	}
	b.ResetTimer()
	bm1.Difference(bm2)
}