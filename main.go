package main

import (
	"fmt"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/xgzlucario/rotom/store"
	"github.com/xgzlucario/rotom/structx"
)

func testTrie() {
	fmt.Println("===== start test Trie =====")
	db := store.DB(0)

	var tree *structx.Trie[int]

	tree, err := store.GetTrie[int](db, "trie")
	if err != nil {
		// not exist
		fmt.Println("get trie error:", err)
		tree = structx.NewTrie[int]()
		for i := 0; i < 999; i++ {
			tree.Put(gofakeit.URL(), i)
		}
		db.Set("trie", tree)
	}

	fmt.Println("size:", tree.Size())

	count := 0
	tree.Walk(func(s string, i int) bool {
		fmt.Println(s, i)
		count++
		return count > 5
	})

	fmt.Println()
}

func testList() {
	fmt.Println("===== start test List =====")
	db := store.DB(0)

	var list *structx.List[int]

	list, err := store.GetList[int](db, "list")
	if err != nil {
		// not exist
		fmt.Println("get list error:", err)
		list = structx.NewList(1)
		for i := 0; i < 10; i++ {
			if i%2 == 0 {
				list.LPush(i)
			} else {
				list.RPush(i)
			}
		}
		db.Set("list", list)
	}

	fmt.Println("list:", list.Values())
	fmt.Println("list sorted:", list.Sort(func(i, j int) bool {
		return i < j
	}).Values())

	fmt.Println()
}

func testValue() {
	fmt.Println("===== start test Value =====")
	db := store.DB(0)

	str, err := db.GetString("str")
	if err != nil {
		fmt.Println(err)
		db.Set("str", "xgz123")
	} else {
		fmt.Println("str:", str)
	}

	t, err := db.GetTime("time")
	if err != nil {
		fmt.Println(err)
		db.Set("time", time.Now())
	} else {
		fmt.Println("time:", t)
	}

	fmt.Println()
	db.Save()
}

func main() {
	testValue()
	testList()
	testTrie()
}
