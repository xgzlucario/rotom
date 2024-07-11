package list

import (
	"testing"
)

func BenchmarkList(b *testing.B) {
	b.Run("lpush", func(b *testing.B) {
		ls := New()
		for i := 0; i < b.N; i++ {
			ls.LPush(genKey(i))
		}
	})
	b.Run("rpush", func(b *testing.B) {
		ls := New()
		for i := 0; i < b.N; i++ {
			ls.RPush(genKey(i))
		}
	})
}

func BenchmarkListPack(b *testing.B) {
	const N = 1000
	b.Run("next", func(b *testing.B) {
		ls := genListPack(0, N)
		it := ls.NewIterator()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			it.SeekBegin().Next()
		}
	})
	b.Run("prev", func(b *testing.B) {
		ls := genListPack(0, N)
		it := ls.NewIterator()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			it.SeekEnd().Prev()
		}
	})
	b.Run("lpush", func(b *testing.B) {
		lp := NewListPack()
		for i := 0; i < 10*10000; i++ {
			lp.LPush("A")
		}
	})
	b.Run("rpush", func(b *testing.B) {
		lp := NewListPack()
		for i := 0; i < 10*10000; i++ {
			lp.RPush("A")
		}
	})
}
