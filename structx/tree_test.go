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

func getRBTree() *RBTree[string, int] {
	tree := NewRBTree[string, int]()
	for i := 0; i < thousand; i++ {
		tree.Insert("xgz"+strconv.Itoa(i), i)
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
func BenchmarkTriePut(b *testing.B) {
	tree := NewTrie[int]()
	for i := 0; i < b.N; i++ {
		tree.Put("xgz"+strconv.Itoa(i), i)
	}
}
func BenchmarkRBTreePut(b *testing.B) {
	tree := NewRBTree[string, int]()
	for i := 0; i < b.N; i++ {
		tree.Insert("xgz"+strconv.Itoa(i), i)
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
func BenchmarkTrieGet(b *testing.B) {
	tree := getTrie()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Get("xgz" + strconv.Itoa(i))
	}
}
func BenchmarkRBTreeGet(b *testing.B) {
	tree := getRBTree()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Find("xgz" + strconv.Itoa(i))
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
func BenchmarkTrieRemove(b *testing.B) {
	tree := getTrie()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Remove("xgz" + strconv.Itoa(i))
	}
}
func BenchmarkRBTreeRemove(b *testing.B) {
	tree := getRBTree()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Delete("xgz" + strconv.Itoa(i))
	}
}
