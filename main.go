package main

import (
	"bytes"
	"fmt"
	"net/http"
	"os/exec"
	"strconv"
	"time"

	_ "net/http/pprof"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/store"
	"github.com/xgzlucario/rotom/structx"
)

var db, _ = store.Open(store.DefaultConfig)

func testStress() {
	a := time.Now()

	// monitor
	var count int64
	go func() {
		for {
			memInfo, _ := mem.VirtualMemory()
			cpuInfo, _ := cpu.Percent(time.Second/5, false)
			dbsize := getDBFileSize()
			fmt.Println("---------------------------------------")
			fmt.Printf("time: %.1fs, count: %d, num: %d\n", time.Since(a).Seconds(), count, db.Size())
			fmt.Printf("mem: %.1f%%, cpu: %.1f%%, db: %.1fM\n", memInfo.UsedPercent, cpuInfo[0], float64(dbsize)/1024/1024)
		}
	}()

	// Simulate testing
	for {
		count++
		val := gofakeit.Animal()

		db.SetEx(gofakeit.Phone(), base.S2B(&val), time.Second*5)
		// db.HSet("hmap", gofakeit.Animal(), base.S2B(&val))
		// db.SetBit("bit", gofakeit.Uint32(), true)
	}
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

func main() {
	db.Set("aa", []byte("12345"))
	fmt.Println(db.HSet("aa", "1", []byte("123")))

	bm, ok := db.Get("bit")
	fmt.Println(bm, ok)

	if bm != nil {
		bm.(*structx.BitMap).Range(func(u uint32) bool {
			fmt.Println(u)
			return false
		})
	}

	for i := 1; i < 9999; i++ {
		db.SetBit("bit", uint32(i), true)
		fmt.Println(i)
		fmt.Println(db.GetBit("bit", uint32(i)))
		fmt.Println(db.GetBit("bit", uint32(i+1)))

		time.Sleep(time.Second)
	}

	db.HSet("hmap", "1", []byte("123"))
	db.HSet("hmap", "2", []byte("123"))
	db.HSet("hmap", "2", []byte("123"))

	time.Sleep(time.Minute)

	// fmt.Println(db.HGet("hmap", "1"))
	// fmt.Println(db.HGet("hmap", "2"))

	go http.ListenAndServe("localhost:6060", nil)
	testStress()
	db.Flush()
}
