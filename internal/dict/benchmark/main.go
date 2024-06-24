package main

import (
	"flag"
	"fmt"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/cockroachdb/swiss"
	"github.com/xgzlucario/rotom/internal/dict"
	"github.com/xgzlucario/rotom/internal/pkg"
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
	flag.IntVar(&entries, "entries", 1000*10000, "number of entries to test.")
	flag.Parse()

	fmt.Println(c)
	fmt.Println("entries:", entries)

	start := time.Now()
	q := pkg.NewQuantile(entries)

	switch c {
	case "dict":
		m := dict.New(dict.DefaultOptions)
		for i := 0; i < entries; i++ {
			k, v := genKV(i)
			start := time.Now()
			m.Set(k, v)
			q.Add(float64(time.Since(start)))
		}

	case "stdmap":
		m := make(map[string][]byte, 8)
		for i := 0; i < entries; i++ {
			k, v := genKV(i)
			start := time.Now()
			m[k] = v
			q.Add(float64(time.Since(start)))
		}

	case "swiss":
		m := swiss.New[string, []byte](8)
		for i := 0; i < entries; i++ {
			k, v := genKV(i)
			start := time.Now()
			m.Put(k, v)
			q.Add(float64(time.Since(start)))
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
	q.Print()
}
