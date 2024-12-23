package list

import (
	"fmt"
	"testing"
)

func genKey(i int) string {
	return fmt.Sprintf("%08x", i)
}

func genList(start, stop int) *QuickList {
	lp := New()
	for i := start; i < stop; i++ {
		lp.RPush(genKey(i))
	}
	return lp
}

func genListPack(start, stop int) *ListPack {
	lp := NewListPack()
	for i := start; i < stop; i++ {
		lp.RPush(genKey(i))
	}
	return lp
}

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
			ls.Range(0, func([]byte) bool {
				return false
			})
		}
	})
}

func BenchmarkListPack(b *testing.B) {
	b.Run("next", func(b *testing.B) {
		lp := genListPack(0, 100)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for it := lp.Iterator(); !it.IsLast(); it.Next() {
			}
		}
	})
	b.Run("prev", func(b *testing.B) {
		lp := genListPack(0, 100)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for it := lp.Iterator().SeekLast(); !it.IsFirst(); it.Prev() {
			}
		}
	})
}
