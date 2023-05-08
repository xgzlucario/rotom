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
)

var db = store.CreateDB(store.DefaultConfig)

func testStress() {
	fmt.Println("===== start test Stress =====")

	a := time.Now()

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
	for {
		count++
		db.SetEX(gofakeit.Phone(), gofakeit.Uint32(), time.Second*5)
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
	go http.ListenAndServe("localhost:6060", nil)

	time.Sleep(time.Second)

	start := time.Now()
	for i := 0; i < 200*10000; i++ {
		db.Set(gofakeit.Phone(), i)
	}
	fmt.Printf("Rotom 批量插入一百万数据耗时: %s\n", time.Since(start))

	// fmt.Println(db.Get("xgz").ToInt())
	// fmt.Println(db.Get("now_time").ToTime())

	// db.Set("xgz", 123)
	// db.Set("now_time", time.Now())

	// fmt.Println(db.Get("xgz").ToInt())
	// fmt.Println(db.Get("now_time").ToTime())

	// testStress()

	RedisTest()

	db.Flush()

	time.Sleep(time.Second)
}
