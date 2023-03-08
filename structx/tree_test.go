package structx

import (
	"strconv"
	"testing"
)

func getTrie() *Trie[struct{}] {
	tree := NewTrie[struct{}]()
	for i := 0; i < 10000; i++ {
		tree.Put("xgz"+strconv.Itoa(i), struct{}{})
	}
	return tree
}

func getRBTree() *RBTree[string, struct{}] {
	tree := NewRBTree[string, struct{}]()
	for i := 0; i < 10000; i++ {
		tree.Insert("xgz"+strconv.Itoa(i), struct{}{})
	}
	return tree
}

func getMap() map[string]struct{} {
	tree := map[string]struct{}{}
	for i := 0; i < 10000; i++ {
		tree["xgz"+strconv.Itoa(i)] = struct{}{}
	}
	return tree
}

// Put
func BenchmarkTriePut(b *testing.B) {
	tree := NewTrie[struct{}]()
	for i := 0; i < b.N; i++ {
		tree.Put("xgz"+strconv.Itoa(i), struct{}{})
	}
}
func BenchmarkRBTreePut(b *testing.B) {
	tree := NewRBTree[string, struct{}]()
	for i := 0; i < b.N; i++ {
		tree.Insert("xgz"+strconv.Itoa(i), struct{}{})
	}
}
func BenchmarkMapPut(b *testing.B) {
	tree := map[string]struct{}{}
	for i := 0; i < b.N; i++ {
		tree["xgz"+strconv.Itoa(i)] = struct{}{}
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
func BenchmarkRBTreeGet(b *testing.B) {
	tree := getRBTree()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Find("xgz" + strconv.Itoa(i))
	}
}
func BenchmarkMapGet(b *testing.B) {
	tree := getMap()
	var s1 struct{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s1 = tree["xgz"+strconv.Itoa(i)]
	}
	if s1 == struct{}{} {
		s1 = struct{}{}
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
func BenchmarkRBTreeRemove(b *testing.B) {
	tree := getRBTree()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Delete("xgz" + strconv.Itoa(i))
	}
}
func BenchmarkMapRemove(b *testing.B) {
	tree := getMap()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		delete(tree, "xgz"+strconv.Itoa(i))
	}
}
