package structx

import (
	"testing"
)

func getBitMap() *BitMap {
	bm := NewBitMap()
	for i := 0; i < 100000000; i++ {
		bm.Add(uint32(i))
	}
	return bm
}

func getRangeBitMaps() (*BitMap, *BitMap) {
	bm := NewBitMap().AddRange(0, 10000)
	bm1 := NewBitMap().AddRange(5000, 15000)
	return bm, bm1
}

func BenchmarkBmAdd(b *testing.B) {
	bm := NewBitMap()
	for i := 0; i < b.N; i++ {
		bm.Add(uint32(i))
	}
}

func BenchmarkBmContains(b *testing.B) {
	bm := getBitMap()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bm.Contains(uint32(i))
	}
}

func BenchmarkBmRemove(b *testing.B) {
	bm := getBitMap()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bm.Remove(uint32(i))
	}
}

func BenchmarkBmMax(b *testing.B) {
	bm := getBitMap()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bm.Max()
	}
}

func BenchmarkBmMin(b *testing.B) {
	bm := getBitMap()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bm.Min()
	}
}

func BenchmarkBmUnion(b *testing.B) {
	bm, bm1 := getRangeBitMaps()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bm.Union(bm1)
	}
}

func BenchmarkBmIntersect(b *testing.B) {
	bm, bm1 := getRangeBitMaps()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bm.Intersect(bm1)
	}
}

func BenchmarkBmDifference(b *testing.B) {
	bm, bm1 := getRangeBitMaps()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bm.Difference(bm1)
	}
}
