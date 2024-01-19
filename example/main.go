package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"time"

	"github.com/xgzlucario/rotom"
)

func main() {
	go http.ListenAndServe("localhost:6060", nil)

	db, err := rotom.Open(rotom.DefaultOptions)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	for i := 0; ; i++ {
		if i%10000 == 0 {
			fmt.Println(i/10000, "w")
		}
		k := strconv.Itoa(i)
		db.SetEx(k, []byte(k), time.Second*10)
	}
}
