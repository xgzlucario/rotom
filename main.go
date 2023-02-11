package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/store"
	"github.com/xgzlucario/rotom/structx"
)

func testTrie() {
	fmt.Println("===== start test Trie =====")
	db := store.DB(0)
	defer db.Save()

	var tree *structx.Trie[int]

	tree, err := store.GetTrie[int](db, "trie")
	if err != nil {
		// not exist
		fmt.Println("get trie error:", err)
		tree = structx.NewTrie[int]()
		for i := 0; i < 9999; i++ {
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
	defer db.Save()

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
	defer db.Save()

	// incr
	fmt.Println(db.Incr("incr-test", 2))

	// string
	str, err := db.GetString("str")
	if err != nil {
		fmt.Println(err)
		db.Set("str", "xgz123")
	} else {
		fmt.Println("str:", str)
	}

	// time.Time
	t, err := db.GetTime("time")
	if err != nil {
		fmt.Println(err)
		db.Set("time", time.Now())
	} else {
		fmt.Println("time:", t)
	}

	fmt.Println()
}

// custom struct
type Stu struct {
	Name string
	Age  int
}

type stuJSON struct {
	N string
	A int
}

func (s *Stu) MarshalJSON() ([]byte, error) {
	return base.MarshalJSON(stuJSON{s.Name, s.Age})
}

func (s *Stu) UnmarshalJSON(src []byte) error {
	var stu stuJSON
	if err := base.UnmarshalJSON(src, &stu); err != nil {
		return err
	}
	s.Name = stu.N
	s.Age = stu.A
	return nil
}

func testCustom() {
	fmt.Println("===== start test Custom =====")

	db := store.DB(0)
	defer db.Save()

	stu, err := store.GetCustomStruct(db, "stu", new(Stu))
	if err != nil {
		fmt.Println(err)
		db.Set("stu", &Stu{"xgz", 22})

	} else {
		fmt.Println(stu, err)
	}
}

func testStress() {
	fmt.Println("===== start test Stress =====")

	db := store.DB(0)
	defer db.Save()

	a := time.Now()
	for i := 0; i < 2000000; i++ {
		db.Set("xgz"+strconv.Itoa(i), i)
		fmt.Println(i)
	}
	fmt.Println("set million data cost:", time.Since(a))
}

func testTTL() {
	fmt.Println("===== start test TTL =====")

	db := store.DB(0)
	defer db.Save()

	db.Set("xgz", "123")
	db.SetWithTTL("xgz-1", "234", time.Second*1)
	db.SetWithTTL("xgz-2", "234", time.Second*3)
	db.SetWithTTL("xgz-3", "234", time.Second*5)

	fmt.Println(db.Keys())
	time.Sleep(time.Second * 2)
	fmt.Println(db.Keys())
	time.Sleep(time.Second * 2)
	fmt.Println(db.Keys())
	time.Sleep(time.Second * 2)
	fmt.Println(db.Keys())
}

func main() {
	testValue()
	testList()
	testTrie()
	testCustom()
	// testTTL()
	// testStress()
}
