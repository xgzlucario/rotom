package rotom

import (
	"fmt"
	"testing"

	"github.com/xgzlucario/rotom/structx"
)

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
	})
	b.Run("set", func(b *testing.B) {
		ls := make([]string, 0, 1024)
		for i := 0; i < 10000; i++ {
			k := fmt.Sprintf("%08x", i)
			ls = append(ls, k)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k := fmt.Sprintf("%08x", i)
			ls[i%10000] = k
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
	b.Run("set", func(b *testing.B) {
		ls := structx.NewList()
		for i := 0; i < 10000; i++ {
			k := fmt.Sprintf("%08x", i)
			ls.LPush(k)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k := fmt.Sprintf("%08x", i)
			ls.Set(i%10000, k)
		}
	})
}

/*
goarch: amd64
pkg: github.com/xgzlucario/rotom
cpu: 13th Gen Intel(R) Core(TM) i5-13600KF
BenchmarkList/lpush-20         	 9250104	       129.6 ns/op	      49 B/op	       4 allocs/op
BenchmarkList/rpush-20         	13763072	        75.28 ns/op	      25 B/op	       2 allocs/op
BenchmarkList/set-20           	 1561099	       764.9 ns/op	      48 B/op	       3 allocs/op
PASS
*/
