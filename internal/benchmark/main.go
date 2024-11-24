package main

import (
	"flag"
	"fmt"
	"github.com/xgzlucario/rotom/internal/zset"
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
	k := fmt.Sprintf("key-%010d", id)
	return k, []byte(k)
}

func genK(id int) string {
	return fmt.Sprintf("key-%010d", id)
}

func main() {
	c := ""
	n := 0
	flag.StringVar(&c, "obj", "hashmap", "")
	flag.IntVar(&n, "n", 512, "")
	flag.Parse()
	fmt.Println(c, n)

	start := time.Now()
	m := map[int]any{}

	switch c {
	case "stdmap":
		for i := 0; i < 10000; i++ {
			hm := map[string][]byte{}
			for j := 0; j < n; j++ {
				k, v := genKV(j)
				hm[k] = v
			}
			m[i] = hm
		}
	case "zipmap":
		for i := 0; i < 10000; i++ {
			hm := hash.New()
			for j := 0; j < n; j++ {
				k, v := genKV(j)
				hm.Set(k, v)
			}
			m[i] = hm
		}
	case "zset":
		for i := 0; i < 10000; i++ {
			zs := zset.New()
			for j := 0; j < n; j++ {
				zs.Set(genK(j), float64(j))
			}
			m[i] = zs
		}
	case "zipzset":
		for i := 0; i < 10000; i++ {
			zs := zset.NewZipZSet()
			for j := 0; j < n; j++ {
				zs.Set(genK(j), float64(j))
			}
			m[i] = zs
		}
	default:
		panic("unknown flags")
	}

	cost := time.Since(start)
	var mem runtime.MemStats
	var stat debug.GCStats

	runtime.ReadMemStats(&mem)
	debug.ReadGCStats(&stat)

	fmt.Println("heap inuse:", mem.HeapInuse/1024/1024, "mb")
	fmt.Println("heap object:", mem.HeapObjects/1024, "k")
	fmt.Println("gc:", stat.NumGC)
	fmt.Println("pause:", gcPause())
	fmt.Println("cost:", cost)
}
