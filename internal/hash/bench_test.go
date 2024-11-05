package hash

import (
	"fmt"
	"testing"
)

const N = 512

func BenchmarkMap(b *testing.B) {
	benchMapI("map", func() MapI { return NewMap() }, b)
	benchMapI("zipmap", func() MapI { return NewZipMap() }, b)
}

func BenchmarkSet(b *testing.B) {
	benchSetI("set", func() SetI { return NewSet() }, b)
	benchSetI("zipset", func() SetI { return NewZipSet() }, b)
}

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

func genSet(s SetI, n int) SetI {
	for i := 0; i < n; i++ {
		s.Add(genKey(i))
	}
	return s
}

func benchMapI(name string, newf func() MapI, b *testing.B) {
	b.Run(name+"/get", func(b *testing.B) {
		m := genMap(newf(), N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.Get(genKey(i % N))
		}
	})
	b.Run(name+"/set", func(b *testing.B) {
		m := newf()
		for i := 0; i < b.N; i++ {
			k := genKey(i % N)
			m.Set(k, []byte(k))
		}
	})
	b.Run(name+"/remove", func(b *testing.B) {
		m := genMap(newf(), b.N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k := genKey(i)
			m.Remove(k)
		}
	})
	b.Run(name+"/scan", func(b *testing.B) {
		m := genMap(newf(), N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.Scan(func(string, []byte) {})
		}
	})
	b.Run(name+"/marshal", func(b *testing.B) {
		m := genMap(newf(), N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = m.Marshal()
		}
	})
}

func benchSetI(name string, newf func() SetI, b *testing.B) {
	b.Run(name+"/add", func(b *testing.B) {
		m := newf()
		for i := 0; i < b.N; i++ {
			m.Add(genKey(i % N))
		}
	})
	b.Run(name+"/exist", func(b *testing.B) {
		m := genSet(newf(), N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k := genKey(i % N)
			m.Exist(k)
		}
	})
	b.Run(name+"/remove", func(b *testing.B) {
		m := genSet(newf(), b.N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k := genKey(i)
			m.Remove(k)
		}
	})
	b.Run(name+"/scan", func(b *testing.B) {
		m := genSet(newf(), N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.Scan(func(string) {})
		}
	})
	b.Run(name+"/marshal", func(b *testing.B) {
		m := genSet(newf(), N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = m.Marshal()
		}
	})
}
