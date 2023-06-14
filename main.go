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

			time.Sleep(time.Nanosecond)
		}
	}()

	// Simulate testing
	for {
		count++
		v := gofakeit.Phone()
		db.SetEx(v, []byte(v), time.Minute)
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
	bc := structx.NewBigCache()
	bc.Set("p1", []byte("[value1]"))

	bc.SetEx("k1", []byte("[value2]"), time.Second*5)
	bc.SetEx("k2", []byte("[value3]"), time.Second*4)
	bc.SetEx("k3", []byte("[value4]"), time.Second*3)
	bc.SetEx("k4", []byte("[value5]"), time.Second*2)
	bc.SetEx("k5", []byte("[value6]"), time.Second)

	for i := 0; i < 10; i++ {
		bc.Print()
		time.Sleep(time.Second)
	}

	// const num = 999999

	// m := structx.NewCache[[]byte]()
	// a := time.Now()
	// for i := 0; i < num; i++ {
	// 	p := gofakeit.Phone()
	// 	m.Set(p, base.S2B(&p))
	// }
	// fmt.Println(time.Since(a))

	// bc := structx.NewBigCache()
	// at := time.Now()
	// for i := 0; i < num; i++ {
	// 	p := gofakeit.Phone()
	// 	bc.Set(p, base.S2B(&p))
	// }
	// fmt.Println(time.Since(at))

	// time.Sleep(time.Hour)

	go http.ListenAndServe("localhost:6060", nil)
	testStress()
	db.Flush()
}
