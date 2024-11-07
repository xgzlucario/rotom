package main

import (
	"flag"
	"fmt"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/xgzlucario/rotom/internal/zset"
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

func genKey(id int) string {
	return fmt.Sprintf("%08x", id)
}

func main() {
	c := ""
	entries := 0
	flag.StringVar(&c, "zset", "zset", "zset to bench.")
	flag.IntVar(&entries, "entries", 2000*10000, "number of entries to test.")
	flag.Parse()

	fmt.Println(c)
	fmt.Println("entries:", entries)

	debug.SetGCPercent(10)
	start := time.Now()

	switch c {
	case "zset":
		m := zset.New()
		for i := 0; i < entries; i++ {
			key := genKey(i)
			m.Set(key, float64(i))
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
}
