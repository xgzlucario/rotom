package main

import (
	"fmt"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/xgzlucario/rotom/store"
	"github.com/xgzlucario/rotom/structx"
)

func main() {
	db := store.DB(0)

	tree := structx.NewTrie[int]()
	for i := 0; i < 999; i++ {
		tree.Put(gofakeit.URL(), i)
	}

	list := structx.NewList(1, 2, 3, 4, 5)
	// db.Set("trie", tree)
	db.Set("list", list)

	tree1, err := store.GetTrie[int](db, "trie")
	tree1.Walk(func(s string, i int) bool {
		fmt.Println(s, i)
		return false
	})
	fmt.Println("===========", tree1, err)

	fmt.Println()
	db.Save()
}
