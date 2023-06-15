package main

import (
	"fmt"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/store"
	"github.com/xgzlucario/rotom/structx"
)

var db, _ = store.Open(&store.Config{
	Path:            "db",
	ShardCount:      32,
	SyncPolicy:      base.Never,
	SyncInterval:    time.Second,
	RewriteInterval: time.Minute,
})

func testStress() {
	a := time.Now()

	var sum, stat float64
	var count int64

	// Monitor
	go func() {
		for {
			time.Sleep(time.Second)
			fmt.Printf("[Cache] %.1fs\t count: %dk\t num: %dk\t avg: %.2f ns\n",
				time.Since(a).Seconds(), count/1000, db.Size()/1000, sum/stat)
		}
	}()

	// Get
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

	// Set
	for {
		count++
		v := gofakeit.Phone()
		db.SetEx(v, []byte(v), time.Second*10)
	}
}

func testStress2() {
	a := time.Now()

	var sum, stat float64
	var count int64

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

	// Monitor
	go func() {
		for {
			time.Sleep(time.Second)
			fmt.Printf("[BigCC] %.1fs\t count: %dk\t num: %dk\t avg: %.2f ns\n",
				time.Since(a).Seconds(), count/1000, getLen()/1000, sum/stat)
		}
	}()

	// Get
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

	// Set
	for i := 0; ; i++ {
		count++
		v := gofakeit.Phone()
		bc[i%n].SetEx(v, []byte(v), time.Second*10)
	}
}

func main() {
	go testStress2()
	go testStress()
	select {}
}
