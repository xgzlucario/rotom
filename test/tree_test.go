package test

import (
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/xgzlucario/rotom/structx"
)

func BenchmarkStdMap(b *testing.B) {
	m := map[string]struct{}{}

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

func BenchmarkHashMap(b *testing.B) {
	m := structx.NewMap[string, struct{}]()

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
