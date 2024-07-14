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
	b.Run("lpop", func(b *testing.B) {
		ls := genList(0, b.N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ls.LPop()
		}
	})
	b.Run("rpop", func(b *testing.B) {
		ls := genList(0, b.N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ls.RPop()
		}
	})
	b.Run("range", func(b *testing.B) {
		ls := genList(0, 100)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ls.Range(0, -1, func([]byte) {})
		}
	})
	b.Run("revrange", func(b *testing.B) {
		ls := genList(0, 100)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ls.RevRange(0, -1, func([]byte) {})
		}
	})
}

func BenchmarkListPack(b *testing.B) {
	b.Run("compress", func(b *testing.B) {
		lp := NewListPack()
		for i := 0; i < 1000; i++ {
			lp.RPush("rotom")
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			lp.Compress()
			lp.Decompress()
		}
	})
	b.Run("replaceBegin", func(b *testing.B) {
		lp := NewListPack()
		for i := 0; i < 1000; i++ {
			lp.RPush("rotom")
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			it := lp.Iterator()
			it.ReplaceNext("abcde")
		}
	})
	b.Run("replaceEnd", func(b *testing.B) {
		lp := NewListPack()
		for i := 0; i < 1000; i++ {
			lp.RPush("rotom")
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			it := lp.Iterator().SeekLast()
			it.Prev()
			it.ReplaceNext("abcde")
		}
	})
}
