package main

import (
	"fmt"
	"strconv"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/xgzlucario/rotom/store"
)

func stressTest() {
	go http.ListenAndServe("localhost:6060", nil)

	db, _ := store.Open(store.DefaultConfig)

	a := time.Now()

	var sum float64
	var stat, count int64

	// Stat
	var maxNum uint64
	go func() {
		for i := 0; ; i++ {
			time.Sleep(time.Second / 10)

			n := db.Stat().Len / 1e3
			if n > maxNum {
				maxNum = n
			}

			if i > 0 && i%100 == 0 {
				fmt.Printf("[Cache] %.0fs\t count: %dk\t num: %dk\t maxNum: %dk\t avg: %.2f ns\n",
					time.Since(a).Seconds(), count/1e3, n, maxNum, sum/float64(stat))
			}
		}
	}()

	// Get
	go func() {
		for i := 0; ; i++ {
			a := time.Now()
			db.Get(strconv.Itoa(i))

			c := time.Since(a).Microseconds()
			sum += float64(c)
			stat++

			time.Sleep(time.Microsecond)

			i %= 1e9
		}
	}()

	// Set
	for i := 0; ; i++ {
		count++
		phone := gofakeit.Phone()
		val := gofakeit.Username()
		db.SetEx(phone, []byte(val), time.Second*5)
	}
}

func main() {
	go http.ListenAndServe("localhost:6060", nil)

	// stressTest()

	db, err := store.Open(store.DefaultConfig)
	if err != nil {
		panic(err)
	}

	if err := db.Listen("0.0.0.0:7676"); err != nil {
		panic(err)
	}
}
