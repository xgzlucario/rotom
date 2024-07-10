package list

import (
	"testing"
)

func BenchmarkListPack(b *testing.B) {
	const N = 1000
	b.Run("next", func(b *testing.B) {
		ls := genListPack(0, N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			it := ls.NewIterator()
			for i := 0; i < N; i++ {
				it.Next()
			}
		}
	})
	b.Run("prev", func(b *testing.B) {
		ls := genListPack(0, N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			it := ls.NewIterator()
			it.SeekEnd()
			for i := 0; i < N; i++ {
				it.Prev()
			}
		}
	})
}
