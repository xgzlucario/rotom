package structx

import (
	"testing"

	"github.com/brianvoe/gofakeit/v6"
)

func getTrie() *Trie[int] {
	tree := NewTrie[int]()
	for i := 0; i < thousand; i++ {
		tree.Put(gofakeit.URL(), i)
	}
	return tree
}

// FakeURL
func BenchmarkFakeURL(b *testing.B) {
	for i := 0; i < b.N; i++ {
		gofakeit.URL()
	}
}

// Add
func BenchmarkTrieAdd(b *testing.B) {
	tree := NewTrie[int]()
	for i := 0; i < b.N; i++ {
		tree.Put(gofakeit.URL(), i)
	}
}

// Get
func BenchmarkTrieGet(b *testing.B) {
	tree := getTrie()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Get(gofakeit.URL())
	}
}

// Walk
func BenchmarkTrieWalk(b *testing.B) {
	tree := getTrie()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Walk(func(s string, i int) bool {
			return false
		})
	}
}

// Contains
func BenchmarkTrieContains(b *testing.B) {
	tree := getTrie()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Contains(gofakeit.URL())
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
