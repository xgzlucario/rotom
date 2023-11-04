package main

import (
	"net/http"
	_ "net/http/pprof"

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
