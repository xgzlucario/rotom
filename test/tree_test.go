package test

import (
	"strconv"
	"testing"

	"github.com/dghubble/trie"
	"github.com/xgzlucario/rotom/structx"
)

func BenchmarkMap(b *testing.B) {
	m := map[string]struct{}{}
	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m["xgz12345678"+strconv.Itoa(i)] = struct{}{}
		}
	})
	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = m["xgz12345678"+strconv.Itoa(i)]
		}
	})
	b.Run("Remove", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			delete(m, "xgz12345678"+strconv.Itoa(i))
		}
	})
}

func BenchmarkTrie(b *testing.B) {
	m := structx.NewTrie[struct{}]()
	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Put("xgz12345678"+strconv.Itoa(i), struct{}{})
		}
	})
	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Get("xgz12345678" + strconv.Itoa(i))
		}
	})
	b.Run("Remove", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Remove("xgz12345678" + strconv.Itoa(i))
		}
	})
}

func BenchmarkRBTree(b *testing.B) {
	m := structx.NewRBTree[string, struct{}]()
	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Insert("xgz12345678"+strconv.Itoa(i), struct{}{})
		}
	})
	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Find("xgz12345678" + strconv.Itoa(i))
		}
	})
	b.Run("Remove", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Delete("xgz12345678" + strconv.Itoa(i))
		}
	})
}

func BenchmarkTrieV2(b *testing.B) {
	m := trie.NewRuneTrie()
	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Put("xgz12345678"+strconv.Itoa(i), struct{}{})
		}
	})
	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Get("xgz12345678" + strconv.Itoa(i))
		}
	})
	b.Run("Remove", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Delete("xgz12345678" + strconv.Itoa(i))
		}
	})
}
