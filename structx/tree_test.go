package structx

import (
	"strconv"
	"testing"
)

func getTrie() *Trie[int] {
	tree := NewTrie[int]()
	for i := 0; i < thousand; i++ {
		tree.Put("xgz"+strconv.Itoa(i), i)
	}
	return tree
}

func getBtree() *Btree[string, int] {
	tree := NewBtree[string, int]()
	for i := 0; i < thousand; i++ {
		tree.Put("xgz"+strconv.Itoa(i), i)
	}
	return tree
}

func getAVLTree() *AVLTree[string, int] {
	tree := NewAVLTree[string, int]()
	for i := 0; i < thousand; i++ {
		tree.Put("xgz"+strconv.Itoa(i), i)
	}
	return tree
}

// Put
func BenchmarkBtreePut(b *testing.B) {
	tree := NewBtree[string, int]()
	for i := 0; i < b.N; i++ {
		tree.Put("xgz"+strconv.Itoa(i), i)
	}
}

// Put
func BenchmarkAVLPut(b *testing.B) {
	tree := NewAVLTree[string, int]()
	for i := 0; i < b.N; i++ {
		tree.Put("xgz"+strconv.Itoa(i), i)
	}
}

// Put
func BenchmarkTriePut(b *testing.B) {
	tree := NewTrie[int]()
	for i := 0; i < b.N; i++ {
		tree.Put("xgz"+strconv.Itoa(i), i)
	}
}

// Get
func BenchmarkBtreeGet(b *testing.B) {
	tree := getBtree()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Get("xgz" + strconv.Itoa(i))
	}
}

// Get
func BenchmarkAVLTreeGet(b *testing.B) {
	tree := getAVLTree()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Get("xgz" + strconv.Itoa(i))
	}
}

// Get
func BenchmarkTrieGet(b *testing.B) {
	tree := getTrie()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Get("xgz" + strconv.Itoa(i))
	}
}

// Remove
func BenchmarkBtreeRemove(b *testing.B) {
	tree := getBtree()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Remove("xgz" + strconv.Itoa(i))
	}
}

// Remove
func BenchmarkAVLTreeRemove(b *testing.B) {
	tree := getAVLTree()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Remove("xgz" + strconv.Itoa(i))
	}
}

// Remove
func BenchmarkTrieRemove(b *testing.B) {
	tree := getTrie()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Remove("xgz" + strconv.Itoa(i))
	}
}
