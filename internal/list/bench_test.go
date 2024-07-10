package list

import (
	"testing"
)

func BenchmarkListPack(b *testing.B) {
	const N = 1000
	b.Run("next", func(b *testing.B) {
		ls := genListPack(0, N)
		it := ls.NewIterator()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			it.SeekBegin()
			it.Next()
		}
	})
	b.Run("prev", func(b *testing.B) {
		ls := genListPack(0, N)
		it := ls.NewIterator()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			it.SeekEnd()
			it.Prev()
		}
	})
	b.Run("lpush", func(b *testing.B) {
		ls := NewListPack()
		it := ls.NewIterator()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			it.SeekBegin()
			it.Insert(genKey(i))
		}
	})
	b.Run("rpush", func(b *testing.B) {
		ls := NewListPack()
		it := ls.NewIterator()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			it.SeekEnd()
			it.Insert(genKey(i))
		}
	})
}
