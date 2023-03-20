package main

import (
	"fmt"
	"math"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/shirou/gopsutil/mem"
	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/store"
	"github.com/xgzlucario/rotom/structx"
)

var db = store.DB()

func testTrie() {
	fmt.Println("===== start test Trie =====")

	tree, err := store.GetTrie[int]("trie")
	if err != nil {
		fmt.Println("get trie error:", err)
		tree = structx.NewTrie[int]()
	}

	for i := 0; i < 10; i++ {
		tree.Put(gofakeit.Name(), gofakeit.Minute())
	}

	fmt.Println(tree.Size(), tree.Keys())
	db.Set("trie", tree)
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

	stu, err := store.GetCustomType("stu", new(Stu))
	if err != nil {
		fmt.Println(err)
		db.Set("stu", &Stu{"xgz", 22})

	} else {
		fmt.Println(stu, err)
	}

	fmt.Println()
}

func testStress() {
	fmt.Println("===== start test Stress =====")

	a := time.Now()
	db.WithExpired(nil)

	fmt.Println("db count is", db.Count())

	// Simulate storing mobile sms code of 100 million users
	for i := 0; i <= 10000*10000; i++ {
		db.SetWithTTL(gofakeit.Phone(), uint16(gofakeit.Number(10000, math.MaxUint16)), time.Second*5)
		// stats
		if i%(10*10000) == 0 {
			memInfo, _ := mem.VirtualMemory()
			fmt.Println("num:", i, "count:", db.Count())
			fmt.Printf("mem usage: %.2f%%\n", memInfo.UsedPercent)
		}
	}
	fmt.Println("total cost:", time.Since(a))
}

func testTTL() {
	fmt.Println("===== start test TTL =====")

	db.WithExpired(func(s string, a any) {
		fmt.Println("exp", s, a)
	})

	db.Set("xgz", "123")
	db.SetWithTTL("xgz-1", "234", time.Second*1)
	db.SetWithTTL("xgz-2", "234", time.Second*3)
	db.SetWithTTL("xgz-3", "234", time.Second*5)

	for i := 0; i < 6; i++ {
		fmt.Println(db.Count())
		time.Sleep(time.Second)
	}

	fmt.Println()
}

func testBloom() {
	fmt.Println("===== start test Bloom =====")

	b, err := db.GetBloom("bloom")
	if err != nil {
		fmt.Println("get bloom error: ", err)

		b = structx.NewBloom()
		b.AddString("xgz123").AddString("qwe456")

		fmt.Println(b.TestString("xgz123"), b.TestString("xgz234"))
		db.Set("bloom", b)

	} else {
		fmt.Println("read bloom success")
		fmt.Println(b.TestString("xgz123"), b.TestString("xgz234"))
	}
	fmt.Println()
}

func main() {
	store.Init()
	// structx.InitAI()

	testBloom()
	testTrie()
	testCustom()
	testTTL()
	testStress()
}
