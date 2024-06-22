package list

import (
	"fmt"
	"testing"
)

func BenchmarkList(b *testing.B) {
	const N = 10000
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
	b.Run("index", func(b *testing.B) {
		ls := genList(0, N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ls.Index(i % N)
		}
	})
	b.Run("set", func(b *testing.B) {
		ls := genList(0, N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ls.Set(i%N, genKey(N-i))
		}
	})
	b.Run("range", func(b *testing.B) {
		ls := genList(0, N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ls.Range(0, -1, func(s []byte) (stop bool) {
				return false
			})
		}
	})
	b.Run("revrange", func(b *testing.B) {
		ls := genList(0, N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ls.RevRange(0, -1, func(s []byte) (stop bool) {
				return false
			})
		}
	})
}

func BenchmarkListPack(b *testing.B) {
	const N = 1000
	b.Run("set/same-len", func(b *testing.B) {
		ls := genListPack(0, N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ls.Set(i%N, fmt.Sprintf("%08x", i))
		}
	})
	b.Run("set/less-len", func(b *testing.B) {
		ls := genListPack(0, N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ls.Set(i%N, fmt.Sprintf("%07x", i))
		}
	})
	b.Run("set/great-len", func(b *testing.B) {
		ls := genListPack(0, N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ls.Set(i%N, fmt.Sprintf("%09x", i))
		}
	})
}
