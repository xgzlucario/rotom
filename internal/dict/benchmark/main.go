package main

import (
	"flag"
	"fmt"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/influxdata/tdigest"
	"github.com/xgzlucario/rotom/internal/dict"
)

var previousPause time.Duration

func gcPause() time.Duration {
	runtime.GC()
	var stats debug.GCStats
	debug.ReadGCStats(&stats)
	pause := stats.PauseTotal - previousPause
	previousPause = stats.PauseTotal
	return pause
}

func genKV(id int) (string, []byte) {
	k := fmt.Sprintf("%08x", id)
	return k, []byte(k)
}

func main() {
	c := ""
	entries := 0
	flag.StringVar(&c, "cache", "dict", "map to bench.")
	flag.IntVar(&entries, "entries", 2000*10000, "number of entries to test.")
	flag.Parse()

	fmt.Println(c)
	fmt.Println("entries:", entries)

	debug.SetGCPercent(10)
	start := time.Now()
	td := tdigest.New()

	switch c {
	case "dict":
		m := dict.New()
		for i := 0; i < entries; i++ {
			k, v := genKV(i)
			start := time.Now()
			m.Set(k, v)
			td.Add(float64(time.Since(start)), 1)
		}

	case "stdmap":
		m := make(map[string]any, 8)
		for i := 0; i < entries; i++ {
			k, v := genKV(i)
			start := time.Now()
			m[k] = v
			td.Add(float64(time.Since(start)), 1)
		}
	}
	cost := time.Since(start)

	var mem runtime.MemStats
	var stat debug.GCStats

	runtime.ReadMemStats(&mem)
	debug.ReadGCStats(&stat)

	fmt.Println("gcsys:", mem.GCSys/1024/1024, "mb")
	fmt.Println("heap inuse:", mem.HeapInuse/1024/1024, "mb")
	fmt.Println("heap object:", mem.HeapObjects/1024, "k")
	fmt.Println("gc:", stat.NumGC)
	fmt.Println("pause:", gcPause())
	fmt.Println("cost:", cost)
	// Compute Quantiles
	fmt.Println("999th:", time.Duration(td.Quantile(0.999)))
}
