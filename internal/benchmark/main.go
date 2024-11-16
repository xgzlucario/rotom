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
	k := fmt.Sprintf("%08x", id)
	return k, []byte(k)
}

func genK(id int) string {
	return fmt.Sprintf("%08x", id)
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
	case "zset":
		for i := 0; i < 10000; i++ {
			zs := zset.New()
			for i := 0; i < 512; i++ {
				zs.Set(genK(i), float64(i))
			}
			m[i] = zs
		}
	case "zipzset":
		for i := 0; i < 10000; i++ {
			zs := zset.NewZipZSet()
			for i := 0; i < 512; i++ {
				zs.Set(genK(i), float64(i))
			}
			m[i] = zs
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
