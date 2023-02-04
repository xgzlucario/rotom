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

func getBtree() *Btree[string, int] {
	tree := NewBtree[string, int]()
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

// Put
func BenchmarkBtreePut(b *testing.B) {
	tree := NewBtree[string, int]()
	for i := 0; i < b.N; i++ {
		tree.Put(gofakeit.URL(), i)
	}
}

// Get
func BenchmarkBtreeGet(b *testing.B) {
	tree := getBtree()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Get(gofakeit.URL())
	}
}

// Remove
func BenchmarkBtreeRemove(b *testing.B) {
	tree := getBtree()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Remove(gofakeit.URL())
	}
}

// Put
func BenchmarkTriePut(b *testing.B) {
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

// Remove
func BenchmarkTrieRemove(b *testing.B) {
	tree := getTrie()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Remove(gofakeit.URL())
	}
}
