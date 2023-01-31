package test

import (
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/xgzlucario/rotom/structx"
)

func getTrie() *structx.Trie[int] {
	tree := structx.NewTrie[int]()
	for i := 0; i < million; i++ {
		tree.Put(gofakeit.URL(), i)
	}
	return tree
}

// Add
func BenchmarkTrieAdd(b *testing.B) {
	tree := structx.NewTrie[int]()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Put(gofakeit.URL(), i)
	}
}

// Walk
func BenchmarkTrieWalk(b *testing.B) {
	tree := getTrie()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Walk(func(key string, value int) bool {
			return false
		})
	}
}

// Delete
func BenchmarkTrieDelete(b *testing.B) {
	tree := getTrie()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Remove(gofakeit.URL())
	}
}
