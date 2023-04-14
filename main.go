package main

import (
	"bytes"
	"fmt"
	"os/exec"
	"strconv"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/bytedance/sonic"
	"github.com/shirou/gopsutil/cpu"
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
	return sonic.Marshal(stuJSON{s.Name, s.Age})
}

func (s *Stu) UnmarshalJSON(src []byte) error {
	var stu stuJSON
	if err := sonic.Unmarshal(src, &stu); err != nil {
		return err
	}
	s.Name = stu.N
	s.Age = stu.A
	return nil
}

func testCustom() {
	fmt.Println("===== start test Custom =====")

	stu, err := store.Get("stu", new(Stu))
	if err != nil {
		fmt.Println("error:", err)
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

	// monitor
	var count int64
	go func() {
		for {
			memInfo, _ := mem.VirtualMemory()
			cpuInfo, _ := cpu.Percent(time.Second/5, false)
			dbsize := getDBFileSize()
			fmt.Println("---------------------------------------")
			fmt.Printf("time: %.1fs, count: %d, num: %d\n", time.Since(a).Seconds(), count, db.Count())
			fmt.Printf("mem: %.1f%%, cpu: %.1f%%, db: %.1fM\n", memInfo.UsedPercent, cpuInfo[0], float64(dbsize)/1024/1024)
		}
	}()

	// Simulate testing
	for i := 0; i < 10000*10000; i++ {
		count++
		db.SetWithTTL(gofakeit.Phone(), gofakeit.Uint32(), time.Second*5)
	}
	fmt.Println("total cost:", time.Since(a))
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

func getDBFileSize() int64 {
	res, err := exec.Command("du", "-s", "db").Output()
	if err != nil {
		return -1
	}
	spr := bytes.IndexByte(res, '\t')
	res = res[:spr]

	num, err := strconv.ParseInt(*base.B2S(res), 10, 64)
	if err != nil {
		return 0
	}
	return num * 1000
}

func testValue() {
	fmt.Println("===== start test Value =====")

	fmt.Println(db.GetUint("uint"))
	fmt.Println(db.GetUint64("uint64"))
	fmt.Println(db.GetUint32("uint32"))
	fmt.Println(db.GetInt("int"))
	fmt.Println(db.GetInt32("int32"))
	fmt.Println(db.GetInt64("int64"))
	fmt.Println(db.GetFloat32("float32"))
	fmt.Println(db.GetFloat64("float64"))
	fmt.Println(db.GetString("string"))
	fmt.Println(db.GetBool("bool"))
	fmt.Println(db.GetTime("time"))
	fmt.Println(db.GetStringSlice("stringSlice"))
	fmt.Println(db.GetIntSlice("intSlice"))

	fmt.Println(db.HGet("xgz", "1"))

	db.Set("uint", uint(123))
	db.Set("uint8", uint8(123))
	db.Set("uint16", uint16(123))
	db.Set("uint32", uint32(123))
	db.Set("uint64", uint64(123))
	db.Set("int", int(123))
	db.Set("int8", int8(123))
	db.Set("int16", int16(123))
	db.Set("int32", int32(123))
	db.Set("int64", int64(123))
	db.Set("float32", float32(123.123))
	db.Set("float64", float64(123.123))
	db.Set("string", "123")
	db.Set("bool", true)
	db.Set("time", time.Now())
	db.Set("stringSlice", []string{"123", "456"})
	db.Set("intSlice", []int{1, 123, 456, 23, 789, 55663})

	db.HSet(123, "xgz", "1")
	db.HSet(456, "xgz", "2")
	fmt.Println(db.HGet("xgz", "1"))
	fmt.Println(db.HGet("xgz", "2"))

	fmt.Println()
}

func main() {
	time.Sleep(time.Second * 2)
	testValue()
	testBloom()
	testTrie()
	testCustom()
	testStress()
	db.Flush()
}
