package main

import (
	"fmt"
	"net/http"
	"os"
	"time"

	_ "net/http/pprof"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/xgzlucario/rotom/store"
	"github.com/xgzlucario/rotom/structx"
	"gonum.org/v1/gonum/stat"
)

var db, _ = store.Open(store.DefaultConfig)

func testStress() {
	a := time.Now()
	stats := make([]float64, 0, 1024)

	// monitor
	var count int64
	go func() {
		for {
			time.Sleep(time.Second)
			// memInfo, _ := mem.VirtualMemory()
			// cpuInfo, _ := cpu.Percent(time.Second/2, false)
			// dbsize := getDBFileSize()
			// fmt.Println("---------------------------------------")
			fmt.Printf(
				"[Cache] time: %.1fs\t count: %d\t num: %d\t avg[%d]: %.2f\n",
				time.Since(a).Seconds(), count, db.Size(), len(stats), stat.Mean(stats, nil))

			// fmt.Printf("mem: %.1f%%, cpu: %.1f%%, db: %.1fM\n", memInfo.UsedPercent, cpuInfo[0], float64(dbsize)/1024/1024)
		}
	}()

	go func() {
		for {
			a := time.Now()
			db.Get(gofakeit.Phone())

			c := time.Since(a).Milliseconds()
			stats = append(stats, float64(c))

			time.Sleep(time.Millisecond)
		}
	}()

	// Simulate testing
	for {
		count++
		v := gofakeit.Phone()
		db.SetEx(v, []byte(v), time.Second*10)
	}
}

func testStress2() {
	a := time.Now()

	const n = 64

	bc := map[int]*structx.BigCache{}
	for i := 0; i < n; i++ {
		bc[i] = structx.NewBigCache()
	}

	getLen := func() (sum int) {
		for _, b := range bc {
			sum += b.Len()
		}
		return sum
	}

	// monitor
	var count int64
	go func() {
		for {
			time.Sleep(time.Second)
			fmt.Printf("[BigCC] time: %.1fs\t count: %d\t num: %d\n", time.Since(a).Seconds(), count, getLen())
			// fmt.Printf("mem: %.1f%%, cpu: %.1f%%, db: %.1fM\n", memInfo.UsedPercent, cpuInfo[0], float64(dbsize)/1024/1024)
		}
	}()

	// go func() {
	// 	for i := 0; ; i++ {
	// 		a := time.Now()
	// 		bc[i%n].Get(gofakeit.Phone())

	// 		c := time.Since(a).Milliseconds()
	// 		if c >= 20 {
	// 			fmt.Printf("[1]===== GET SLOW: %d ms =====\n", c)
	// 		}

	// 		time.Sleep(time.Nanosecond)
	// 	}
	// }()

	// Simulate testing
	for i := 0; ; i++ {
		count++
		v := gofakeit.Phone()
		bc[i%n].SetEx(v, []byte(v), time.Second*10)
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
	go testStress2()
	go testStress()
	select {}
}
