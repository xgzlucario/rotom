package hash

import (
	"fmt"
	"testing"
)

func genKey(i int) string {
	return fmt.Sprintf("%08x", i)
}

func BenchmarkMap(b *testing.B) {
	benchMapI("map", func() MapI { return NewMap() }, b)
	benchMapI("zipmap", func() MapI { return NewZipMap() }, b)
}

func benchMapI(name string, newf func() MapI, b *testing.B) {
	b.Run(name+"-set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m := newf()
			for i := 0; i < 512; i++ {
				k := genKey(i)
				m.Set(k, []byte(k))
			}
		}
	})
	b.Run(name+"-update", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m := newf()
			for i := 0; i < 512; i++ {
				k := genKey(0)
				m.Set(k, []byte(k))
			}
		}
	})
}

func BenchmarkSet(b *testing.B) {
	benchSetI("set", func() SetI { return NewSet() }, b)
	benchSetI("zipset", func() SetI { return NewZipSet() }, b)
}

func benchSetI(name string, newf func() SetI, b *testing.B) {
	b.Run(name+"-add", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m := newf()
			for i := 0; i < 512; i++ {
				m.Add(genKey(i))
			}
		}
	})
}
