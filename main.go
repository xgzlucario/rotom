package main

import (
	"net/http"
	_ "net/http/pprof"

	"github.com/xgzlucario/rotom/store"
)

const (
	GB = 1024 * 1024 * 1024
)

func main() {
	go http.ListenAndServe("localhost:6060", nil)

	db, err := store.Open(store.DefaultConfig)
	if err != nil {
		panic(err)
	}

	// Monitor
	// go func() {
	// 	var memStats runtime.MemStats
	// 	for {
	// 		time.Sleep(time.Second * 5)
	// 		runtime.ReadMemStats(&memStats)

	// 		// print GC stats
	// 		fmt.Printf("[GC] times: %d, alloc: %.2f GB, heapObj: %d k, pause: %v ms\n",
	// 			memStats.NumGC,
	// 			float64(memStats.Alloc)/GB,
	// 			memStats.HeapObjects/1e3,
	// 			memStats.PauseNs[(memStats.NumGC+255)%256]/1000,
	// 		)

	// 		// print db stats
	// 		fmt.Println("[Stat]", db.Stat())
	// 	}
	// }()

	if err := db.Listen("0.0.0.0:7676"); err != nil {
		panic(err)
	}
}
