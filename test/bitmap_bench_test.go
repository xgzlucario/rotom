package test

import (
	"testing"

	"github.com/xgzlucario/rotom/structx"
)

func BenchmarkBitMap(b *testing.B) {
	bm := structx.NewBitMap()

	b.Run("Add", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bm.Add(uint32(i))
		}
	})
	b.Run("Contains", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bm.Contains(uint32(i))
		}
	})
	b.Run("Min", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bm.Min()
		}
	})
	b.Run("Max", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bm.Max()
		}
	})

	bm1 := bm.Copy()

	b.Run("Union", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bm.Union(bm1)
		}
	})
	b.Run("Intersect", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bm.Intersect(bm1)
		}
	})
	b.Run("Difference", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bm.Difference(bm1)
		}
	})
	b.Run("Remove", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bm.Remove(uint32(i))
		}
	})
}
