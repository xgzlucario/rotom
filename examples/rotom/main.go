package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"strconv"

	"github.com/xgzlucario/rotom"
)

const (
	GB = 1024 * 1024 * 1024
)

func main() {
	go http.ListenAndServe("localhost:6060", nil)

	db, err := rotom.Open(rotom.DefaultConfig)
	if err != nil {
		panic(err)
	}

	key := "key"

	for j := 0; j < 20; j++ {
		db.RPush(key, strconv.Itoa(j))
	}
	for j := 0; j < 20; j++ {
		res, err := db.LPop(key)
		fmt.Println(res, err)
	}

	// LLen
	num, err := db.LLen(key)
	fmt.Println(num, err)

	// run for web server
	if err := db.Listen("0.0.0.0:7676"); err != nil {
		panic(err)
	}

	// or run for local
	// db, err := rotom.Open(rotom.DefaultConfig)
	// if err != nil {
	// 	panic(err)
	// }
	// defer db.Close()

	// for i := 0; ; i++ {
	// 	k := strconv.Itoa(i)
	// 	db.SetEx(k, []byte(k), time.Second*10)
	// }
}
