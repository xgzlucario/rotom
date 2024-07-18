package main

import (
	"flag"
	"fmt"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/xgzlucario/rotom/internal/hash"
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
	flag.StringVar(&c, "obj", "hashmap", "object to bench.")
	flag.Parse()
	fmt.Println(c)

	start := time.Now()
	m := map[int]any{}

	switch c {
	case "hashmap":
		for i := 0; i < 10000; i++ {
			hm := hash.NewMap()
			for i := 0; i < 512; i++ {
				k, v := genKV(i)
				hm.Set(k, v)
			}
			m[i] = hm
		}

	case "zipmap":
		for i := 0; i < 10000; i++ {
			hm := hash.NewZipMap()
			for i := 0; i < 512; i++ {
				k, v := genKV(i)
				hm.Set(k, v)
			}
			m[i] = hm
		}

	case "zipmap-compressed":
		for i := 0; i < 10000; i++ {
			hm := hash.NewZipMap()
			for i := 0; i < 512; i++ {
				k, v := genKV(i)
				hm.Set(k, v)
			}
			hm.Compress()
			m[i] = hm
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
