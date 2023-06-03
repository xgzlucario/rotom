package main

import (
	"fmt"
	"net/http"
	"os"
	"time"

	_ "net/http/pprof"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
	"github.com/xgzlucario/rotom/store"
)

var db, _ = store.Open(store.DefaultConfig)

func testStress() {
	a := time.Now()

	// monitor
	var count int64
	go func() {
		for {
			memInfo, _ := mem.VirtualMemory()
			cpuInfo, _ := cpu.Percent(time.Second/2, false)
			dbsize := getDBFileSize()
			fmt.Println("---------------------------------------")
			fmt.Printf("time: %.1fs, count: %d, num: %d\n", time.Since(a).Seconds(), count, db.Size())
			fmt.Printf("mem: %.1f%%, cpu: %.1f%%, db: %.1fM\n", memInfo.UsedPercent, cpuInfo[0], float64(dbsize)/1024/1024)
		}
	}()

	go func() {
		for {
			a := time.Now()
			db.Get(gofakeit.Phone())

			c := time.Since(a).Milliseconds()
			if c >= 20 {
				fmt.Printf("===== GET SLOW: %d ms =====\n", c)
			}

			time.Sleep(time.Millisecond * 10)
		}
	}()

	// Simulate testing
	for {
		count++
		db.SetEx(gofakeit.Phone(), []byte{'a', 'b', 'c', 'd'}, time.Second*10)
	}
}

func getDBFileSize() (count int64) {
	if files, err := os.ReadDir("db"); err != nil {
		return -1
	} else {
		for _, file := range files {
			info, err := file.Info()
			if err == nil {
				count += info.Size()
			}
		}
		return
	}
}

func main() {
	go http.ListenAndServe("localhost:6060", nil)
	testStress()
	db.Flush()
}
