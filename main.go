package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"time"

	"github.com/xgzlucario/rotom/store"
)

func main() {
	go http.ListenAndServe("localhost:6060", nil)

	db, err := store.Open(store.DefaultConfig)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			time.Sleep(time.Second * 5)
			fmt.Println(db.Stat())
		}
	}()

	if err := db.Listen("0.0.0.0:7676"); err != nil {
		panic(err)
	}
}
