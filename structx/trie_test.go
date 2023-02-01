package structx

import (
	"testing"

	"github.com/brianvoe/gofakeit/v6"
)

func getTrie() *Trie[int] {
	tree := NewTrie[int]()
	for i := 0; i < million; i++ {
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

func BenchmarkMapAdd(b *testing.B) {
	m := make(map[string]int)
	for i := 0; i < b.N; i++ {
		m[gofakeit.URL()] = i
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

// Keys
func BenchmarkTrieKeys(b *testing.B) {
	tree := getTrie()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Keys()
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
