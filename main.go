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
		db.SetEx(gofakeit.Phone(), []byte{'1'}, time.Second*5)
		// db.BitSet("bit", uint(rand.Uint32()), true)
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
	bs := structx.NewBitset()
	bs.FlipRange(5, 100)
	fmt.Println(bs)

	db.BitSet("bit1", 1, true)
	db.BitSet("bit1", 2, true)

	db.BitSet("bit2", 2, true)
	db.BitSet("bit2", 3, true)
	fmt.Println(db.BitAnd("bit1", "bit2", "bit3"))

	// db.Set("aa", []byte("12345"))
	// fmt.Println(db.HSet("aa", "1", []byte("123")))

	bm, ok := db.GetAny("bit3")
	fmt.Println(bm, ok)

	if bm != nil {
		fmt.Println("BITSET:", bm.(*structx.Bitset).String())
	}

	fmt.Println(db.HGet("hmap", "1"))
	fmt.Println(db.HGet("hmap", "2"))

	db.HSet("hmap", "1", []byte("123"))
	db.HSet("hmap", "2", []byte("234"))
	db.HSet("hmap", "2", []byte("345"))

	go http.ListenAndServe("localhost:6060", nil)
	testStress()
	db.Flush()
}
