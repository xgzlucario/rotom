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

// Put
func BenchmarkBtreePut(b *testing.B) {
	tree := NewBtree[string, int]()
	for i := 0; i < b.N; i++ {
		tree.Put(getFakeURL(b), i)
	}
}

// Get
func BenchmarkBtreeGet(b *testing.B) {
	tree := getBtree()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Get(getFakeURL(b))
	}
}

// Remove
func BenchmarkBtreeRemove(b *testing.B) {
	tree := getBtree()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Remove(getFakeURL(b))
	}
}

// Put
func BenchmarkTriePut(b *testing.B) {
	tree := NewTrie[int]()
	for i := 0; i < b.N; i++ {
		tree.Put(getFakeURL(b), i)
	}
}

// Get
func BenchmarkTrieGet(b *testing.B) {
	tree := getTrie()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Get(getFakeURL(b))
	}
}

// Remove
func BenchmarkTrieRemove(b *testing.B) {
	tree := getTrie()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Remove(getFakeURL(b))
	}
}
