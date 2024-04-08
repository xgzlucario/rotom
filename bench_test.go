package rotom

import (
	"fmt"
	"testing"

	"github.com/xgzlucario/rotom/structx"
)

func getSlice(n int) []string {
	ls := make([]string, 0, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("%08x", i)
		ls = append(ls, k)
	}
	return ls
}

func getList(n int) *structx.List {
	ls := structx.NewList()
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("%08x", i)
		ls.RPush(k)
	}
	return ls
}

func BenchmarkSlice(b *testing.B) {
	b.Run("lpush", func(b *testing.B) {
		ls := make([]string, 0, 1024)
		for i := 0; i < b.N; i++ {
			k := fmt.Sprintf("%08x", i)
			ls = append([]string{k}, ls...)
		}
	})
	b.Run("rpush", func(b *testing.B) {
		ls := make([]string, 0, 1024)
		for i := 0; i < b.N; i++ {
			k := fmt.Sprintf("%08x", i)
			ls = append(ls, k)
		}
		_ = len(ls)
	})
	b.Run("set", func(b *testing.B) {
		ls := getSlice(10000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k := fmt.Sprintf("%08x", i)
			ls[i%10000] = k
		}
	})
	b.Run("iter", func(b *testing.B) {
		ls := getSlice(10000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, k := range ls {
				_ = k
			}
		}
	})
}

func BenchmarkList(b *testing.B) {
	b.Run("lpush", func(b *testing.B) {
		ls := structx.NewList()
		for i := 0; i < b.N; i++ {
			k := fmt.Sprintf("%08x", i)
			ls.LPush(k)
		}
	})
	b.Run("rpush", func(b *testing.B) {
		ls := structx.NewList()
		for i := 0; i < b.N; i++ {
			k := fmt.Sprintf("%08x", i)
			ls.RPush(k)
		}
	})
	b.Run("lpop", func(b *testing.B) {
		ls := getList(b.N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ls.LPop()
		}
	})
	b.Run("rpop", func(b *testing.B) {
		ls := getList(b.N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ls.RPop()
		}
	})
	b.Run("set", func(b *testing.B) {
		ls := getList(10000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k := fmt.Sprintf("%08x", i)
			ls.Set(i%10000, k)
		}
	})
	b.Run("iter", func(b *testing.B) {
		ls := getList(10000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ls.Range(0, -1, func(s string) (stop bool) {
				return false
			})
		}
	})
}
