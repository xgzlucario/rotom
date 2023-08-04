package main

import (
	"fmt"
	"net/http"
	"strconv"

	_ "net/http/pprof"

	"github.com/xgzlucario/rotom/store"
)

func testStress() {
	m, err := store.Open(store.DefaultConfig)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 456; i++ {
		m.Set("xgz"+strconv.Itoa(i), []byte{3, 6, 4})
	}

	fmt.Println(m.Get("xgz133"))
	fmt.Println(m.Get("xgz242"))
	fmt.Println(m.Get("xgz334"))
}

func main() {
	go http.ListenAndServe("localhost:6060", nil)
	go testStress()
	select {}
}
