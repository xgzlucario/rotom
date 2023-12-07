package main

import (
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"time"

	"github.com/xgzlucario/rotom"
)

func main() {
	go http.ListenAndServe("localhost:6060", nil)

	db, err := rotom.Open(rotom.DefaultConfig)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	for i := 0; ; i++ {
		k := strconv.Itoa(i)
		db.SetEx(k, []byte(k), time.Second)
	}
}