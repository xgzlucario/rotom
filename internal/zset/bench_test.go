package zset

import (
	"fmt"
	"github.com/xgzlucario/rotom/internal/iface"
	"testing"
)

const N = 512

func BenchmarkZSet(b *testing.B) {
	benchZSetI("zset", func() iface.ZSetI { return New() }, b)
	benchZSetI("zipzset", func() iface.ZSetI { return NewZipZSet() }, b)
}

func genKey(i int) string {
	return fmt.Sprintf("%08x", i)
}

func genZSet(m iface.ZSetI, n int) iface.ZSetI {
	for i := 0; i < n; i++ {
		k := genKey(i)
		m.Set(k, float64(i%N))
	}
	return m
}

func benchZSetI(name string, newf func() iface.ZSetI, b *testing.B) {
	b.Run(name+"/get", func(b *testing.B) {
		m := genZSet(newf(), N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.Get(genKey(i % N))
		}
	})
	b.Run(name+"/set", func(b *testing.B) {
		m := newf()
		for i := 0; i < b.N; i++ {
			m.Set(genKey(i%N), float64(i%N))
		}
	})
	b.Run(name+"/remove", func(b *testing.B) {
		m := genZSet(newf(), N)
		b.ResetTimer()
		for i := 0; i < N; i++ {
			k := genKey(i)
			m.Remove(k)
		}
	})
	b.Run(name+"/scan", func(b *testing.B) {
		m := genZSet(newf(), N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.Scan(func(string, float64) {})
		}
	})
}
