package main

import (
	"net/http"
	_ "net/http/pprof"

	"github.com/xgzlucario/rotom/store"
)

func main() {
	go http.ListenAndServe("localhost:6060", nil)

	db, err := store.Open(store.DefaultConfig)
	if err != nil {
		panic(err)
	}

	if err := db.Listen("0.0.0.0:7676"); err != nil {
		panic(err)
	}
}
