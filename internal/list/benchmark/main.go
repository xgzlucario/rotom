package main

import (
	"flag"
	"fmt"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/xgzlucario/quicklist"
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
	flag.StringVar(&c, "list", "[]string", "list to bench.")
	flag.IntVar(&entries, "entries", 2000*10000, "number of entries to test.")
	flag.Parse()

	fmt.Println(c)
	fmt.Println("entries:", entries)

	start := time.Now()
	switch c {
	case "[]string":
		ls := make([]string, 0)
		for i := 0; i < entries; i++ {
			ls = append(ls, genKey(i))
		}
		defer func() {
			_ = len(ls)
		}()

	case "quicklist":
		ls := quicklist.New()
		for i := 0; i < entries; i++ {
			ls.RPush(genKey(i))
		}
		defer func() {
			_ = ls.Size()
		}()
	}
	cost := time.Since(start)

	var mem runtime.MemStats
	var stat debug.GCStats

	runtime.ReadMemStats(&mem)
	debug.ReadGCStats(&stat)

	fmt.Println("alloc:", mem.Alloc/1024/1024, "mb")
	fmt.Println("gcsys:", mem.GCSys/1024/1024, "mb")
	fmt.Println("heap inuse:", mem.HeapInuse/1024/1024, "mb")
	fmt.Println("heap object:", mem.HeapObjects/1024, "k")
	fmt.Println("gc:", stat.NumGC)
	fmt.Println("pause:", gcPause())
	fmt.Println("cost:", cost)
}
