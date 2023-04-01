package test

import (
	"strconv"
	"testing"

	"github.com/xgzlucario/rotom/structx"
)

func getTrie() *structx.Trie[struct{}] {
	tree := structx.NewTrie[struct{}]()
	for i := 0; i < 10000; i++ {
		tree.Put("xgz"+strconv.Itoa(i), struct{}{})
	}
	return tree
}

func getRBTree() *structx.RBTree[string, struct{}] {
	tree := structx.NewRBTree[string, struct{}]()
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
func Benchmark_TriePut(b *testing.B) {
	tree := structx.NewTrie[struct{}]()
	for i := 0; i < b.N; i++ {
		tree.Put("xgz"+strconv.Itoa(i), struct{}{})
	}
}
func Benchmark_RBTreePut(b *testing.B) {
	tree := structx.NewRBTree[string, struct{}]()
	for i := 0; i < b.N; i++ {
		tree.Insert("xgz"+strconv.Itoa(i), struct{}{})
	}
}
func Benchmark_MapPut(b *testing.B) {
	tree := map[string]struct{}{}
	for i := 0; i < b.N; i++ {
		tree["xgz"+strconv.Itoa(i)] = struct{}{}
	}
}

// Get
func Benchmark_TrieGet(b *testing.B) {
	tree := getTrie()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Get("xgz" + strconv.Itoa(i))
	}
}
func Benchmark_RBTreeGet(b *testing.B) {
	tree := getRBTree()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Find("xgz" + strconv.Itoa(i))
	}
}
func Benchmark_MapGet(b *testing.B) {
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
