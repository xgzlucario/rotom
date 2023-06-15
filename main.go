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
)

var db, _ = store.Open(store.DefaultConfig)

func testStress() {
	a := time.Now()

	var sum float64
	var stat, count int64

	go func() {
		for {
			time.Sleep(time.Second)
			fmt.Printf("[Cache] %.1fs\t count: %dk\t num: %dk\t avg: %.2f ns\n",
				time.Since(a).Seconds(), count/1000, db.Size()/1000, sum/float64(stat))
		}
	}()

	go func() {
		for {
			a := time.Now()
			db.Get(gofakeit.Phone())

			c := time.Since(a).Microseconds()
			sum += float64(c)
			stat++

			time.Sleep(time.Millisecond)
		}
	}()

	for {
		count++
		v := gofakeit.Phone()
		db.SetEx(v, []byte(v), time.Second*10)
	}
}

func testStress2() {
	a := time.Now()

	var sum float64
	var stat, count int64

	const n = 512

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

	go func() {
		for {
			time.Sleep(time.Second)
			fmt.Printf("[BigCC] %.1fs\t count: %dk\t num: %dk\t avg: %.2f ns\n",
				time.Since(a).Seconds(), count/1000, getLen()/1000, sum/float64(stat))
		}
	}()

	go func() {
		for i := 0; ; i++ {
			a := time.Now()
			bc[i%n].Get(gofakeit.Phone())

			c := time.Since(a).Microseconds()
			sum += float64(c)
			stat++

			time.Sleep(time.Millisecond)
		}
	}()

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
