package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"runtime/debug"
	"time"

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

	// Monitor
	go func() {
		var stats debug.GCStats
		var memStats runtime.MemStats

		for {
			time.Sleep(time.Second * 5)

			debug.ReadGCStats(&stats)
			runtime.ReadMemStats(&memStats)

			// print GC stats
			fmt.Printf("[GC] times: %d, alloc: %.2f GB, sys: %.2f GB, heapObj: %d k, pause: %v\n",
				stats.NumGC,
				float64(memStats.Alloc)/GB,
				float64(memStats.Sys)/GB,
				memStats.HeapObjects/1e3,
				stats.PauseTotal/time.Duration(stats.NumGC),
			)

			// print db stats
			fmt.Println("[Stat]", db.Stat())
		}
	}()

	if err := db.Listen("0.0.0.0:7676"); err != nil {
		panic(err)
	}
}
