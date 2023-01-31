package main

import (
	"github.com/brianvoe/gofakeit/v6"
	"github.com/xgzlucario/rotom/store"
	"github.com/xgzlucario/rotom/structx"
)

func main() {
	tree := structx.NewTrie[int]()
	for i := 0; i < 3999999; i++ {
		tree.Put(gofakeit.URL(), i)
	}
	store.DB(0).Set("trie", tree)
}
