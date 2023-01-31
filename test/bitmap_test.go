package test

import (
	"testing"

	"github.com/xgzlucario/rotom/structx"
)

func getBitMap() *structx.BitMap {
	bm := structx.NewBitMap()
	for i := 0; i < billion; i++ {
		bm.Add(uint32(i))
	}
	return bm
}

func getRangeBitMaps() (*structx.BitMap, *structx.BitMap) {
	bm := structx.NewBitMap().AddRange(0, 10000)
	bm1 := structx.NewBitMap().AddRange(5000, 15000)
	return bm, bm1
}

func BenchmarkBmAdd(b *testing.B) {
	bm := structx.NewBitMap()
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

func BenchmarkBmUnionInplace(b *testing.B) {
	bm, bm1 := getRangeBitMaps()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bm.Union(bm1, true)
	}
}

func BenchmarkBmIntersect(b *testing.B) {
	bm, bm1 := getRangeBitMaps()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bm.Intersect(bm1)
	}
}

func BenchmarkBmIntersectInplace(b *testing.B) {
	bm, bm1 := getRangeBitMaps()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bm.Intersect(bm1, true)
	}
}

func BenchmarkBmDifference(b *testing.B) {
	bm, bm1 := getRangeBitMaps()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bm.Difference(bm1)
	}
}

func BenchmarkBmDifferenceInplace(b *testing.B) {
	bm, bm1 := getRangeBitMaps()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bm.Difference(bm1, true)
	}
}

func BenchmarkBmMarshal(b *testing.B) {
	bm := getBitMap()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bm.MarshalJSON()
	}
}
