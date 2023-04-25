package test

import (
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/tidwall/hashmap"
	"github.com/xgzlucario/rotom/structx"
)

func BenchmarkMap(b *testing.B) {
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

func BenchmarkHashmap(b *testing.B) {
	var m hashmap.Map[string, struct{}]
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
			m.Put(gofakeit.Phone(), struct{}{})
		}
	})
	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Get(gofakeit.Phone())
		}
	})
	b.Run("Remove", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Remove(gofakeit.Phone())
		}
	})
}

func BenchmarkRBTree(b *testing.B) {
	m := structx.NewRBTree[string, struct{}]()
	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Insert(gofakeit.Phone(), struct{}{})
		}
	})
	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Find(gofakeit.Phone())
		}
	})
	b.Run("Remove", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Delete(gofakeit.Phone())
		}
	})
}

func BenchmarkMMap(b *testing.B) {
	m := structx.NewHMap()
	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.HSet(struct{}{}, gofakeit.Animal(), gofakeit.Animal(), gofakeit.Animal())
		}
	})
	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.HGet(gofakeit.Animal(), gofakeit.Animal(), gofakeit.Animal())
		}
	})
	b.Run("Remove", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.HRemove(gofakeit.Animal(), gofakeit.Animal(), gofakeit.Animal())
		}
	})
}
