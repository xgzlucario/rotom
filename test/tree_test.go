package test

import (
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/tidwall/hashmap"
	"github.com/xgzlucario/rotom/structx"
)

const (
	l1 = 1000
	l2 = l1 * 1000
)

func benchMaper(b *testing.B, m map[string]struct{}) {
	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m[gofakeit.Phone()] = struct{}{}
		}
	})
	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = m[gofakeit.Phone()]
		}
	})
	b.Run("Remove", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			delete(m, gofakeit.Phone())
		}
	})
}

func Benchmark_Map(b *testing.B) {
	benchMaper(b, map[string]struct{}{})
}

func Benchmark_Map_l1(b *testing.B) {
	m := map[string]struct{}{}
	for i := 0; i < l1; i++ {
		m[gofakeit.Phone()] = struct{}{}
	}
	benchMaper(b, m)
}

func Benchmark_Map_l2(b *testing.B) {
	m := map[string]struct{}{}
	for i := 0; i < l2; i++ {
		m[gofakeit.Phone()] = struct{}{}
	}
	benchMaper(b, m)
}

func benchHashMap(b *testing.B, m hashmap.Map[string, struct{}]) {
	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Set(gofakeit.Phone(), struct{}{})
		}
	})
	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Get(gofakeit.Phone())
		}
	})
	b.Run("Remove", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Delete(gofakeit.Phone())
		}
	})
}

func Benchmark_Hashmap(b *testing.B) {
	var m hashmap.Map[string, struct{}]
	benchHashMap(b, m)
}

func Benchmark_Hashmap_l1(b *testing.B) {
	var m hashmap.Map[string, struct{}]
	for i := 0; i < l1; i++ {
		m.Set(gofakeit.Phone(), struct{}{})
	}
	benchHashMap(b, m)
}

func Benchmark_Hashmap_l2(b *testing.B) {
	var m hashmap.Map[string, struct{}]
	for i := 0; i < l2; i++ {
		m.Set(gofakeit.Phone(), struct{}{})
	}
	benchHashMap(b, m)
}

func BenchmarkTrie(b *testing.B) {
	m := structx.NewTrie[struct{}]()
	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Set(gofakeit.Phone(), struct{}{})
		}
	})
	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Get(gofakeit.Phone())
		}
	})
	b.Run("Remove", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Delete(gofakeit.Phone())
		}
	})
}
