

package test

import (
	"math/rand"
	"testing"

	"github.com/xgzlucario/rotom/structx"
)

func BenchmarkRBTreeInsert(b *testing.B) {
	tree := structx.NewRBTree[int, int]()
	for i := 0; i < b.N; i++ {
		tree.Insert(rand.Intn(b.N), i)
	}
}

func BenchmarkRBTreeFind(b *testing.B) {
	tree := structx.NewRBTree[int, int]()
	for i := 0; i < b.N; i++ {
		tree.Insert(rand.Intn(b.N), i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Find(rand.Intn(b.N))
	}
}

func BenchmarkRBTreeDelete(b *testing.B) {
	tree := structx.NewRBTree[int, int]()
	for i := 0; i < b.N; i++ {
		tree.Insert(rand.Intn(b.N), i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Delete(rand.Intn(b.N))
	}
}