package hash

import (
	"fmt"
	"testing"
)

func genKey(i int) string {
	return fmt.Sprintf("%08x", i)
}

func genMap(m MapI, n int) MapI {
	for i := 0; i < n; i++ {
		k := genKey(i)
		m.Set(k, []byte(k))
	}
	return m
}

func BenchmarkMap(b *testing.B) {
	benchMapI("map", func() MapI { return NewMap() }, b)
	benchMapI("zipmap", func() MapI { return NewZipMap() }, b)
}

func benchMapI(name string, newf func() MapI, b *testing.B) {
	const N = 512
	b.Run(name+"/set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			genMap(newf(), N)
		}
	})
	b.Run(name+"/update", func(b *testing.B) {
		m := genMap(newf(), N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k := genKey(i % N)
			m.Set(k, []byte(k))
		}
	})
	b.Run(name+"/scan", func(b *testing.B) {
		m := genMap(newf(), N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.Scan(func(string, []byte) {})
		}
	})
}
