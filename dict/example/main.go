package main

import (
	"fmt"
	"math/rand/v2"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"slices"
	"strconv"
	"time"

	"github.com/xgzlucario/rotom/dict"
)

type Quantile struct {
	f []float64
}

func NewQuantile(size int) *Quantile {
	return &Quantile{f: make([]float64, 0, size)}
}

func (q *Quantile) Add(v float64) {
	q.f = append(q.f, v)
}

func (q *Quantile) quantile(p float64) float64 {
	r := q.f[int(float64(len(q.f))*p)]
	return r
}

func (q *Quantile) Print() {
	slices.Sort(q.f)
	fmt.Printf("90th: %.0f ns\n", q.quantile(0.9))
	fmt.Printf("99th: %.0f ns\n", q.quantile(0.99))
	fmt.Printf("999th: %.0f ns\n", q.quantile(0.999))
}

const N = 100 * 10000

func main() {
	go func() {
		_ = http.ListenAndServe("localhost:6060", nil)
	}()

	options := dict.DefaultOptions

	fmt.Println("=====Options=====")
	fmt.Printf("%+v\n", options)
	benchmark(options)
	runtime.GC()
}

func benchmark(options dict.Options) {
	quant := NewQuantile(N)

	var count int64
	var memStats runtime.MemStats

	dict := dict.New(options)

	// Set test
	start := time.Now()
	var now time.Time
	for j := 0; ; j++ {
		k := strconv.FormatUint(rand.Uint64(), 36)

		if j%10 == 0 {
			now = time.Now()
			if now.Sub(start) > time.Minute {
				break
			}
		}

		dict.SetEx(k, []byte(k), time.Second)
		count++

		if j%10 == 0 {
			cost := float64(time.Since(now)) / float64(time.Nanosecond)
			quant.Add(cost)
		}

		// evict
		if j%10 == 0 {
			dict.EvictExpired()
		}
	}

	// Stat
	stat := dict.GetStats()

	fmt.Printf("[Cache] %.0fs | %dw | len: %dw | alloc: %v (unused: %.1f%%)\n",
		time.Since(start).Seconds(),
		count/1e4,
		stat.Len/1e4,
		formatSize(stat.Alloc),
		stat.UnusedRate(),
	)
	fmt.Printf("[Evict] probe: %vw / %vw (%.1f%%) | mgr: %d\n",
		stat.Evictions/1e5, stat.Probes/1e5, stat.EvictionRate(),
		stat.Migrates)

	// mem stats
	runtime.ReadMemStats(&memStats)
	fmt.Printf("[Mem] mem: %.0fMB | sys: %.0fMB | gc: %d | gcpause: %.0f us\n",
		float64(memStats.Alloc)/1024/1024,
		float64(memStats.Sys)/1024/1024,
		memStats.NumGC,
		float64(memStats.PauseTotalNs)/float64(memStats.NumGC)/1000)

	// quant print
	quant.Print()

	fmt.Println("-----------------------------------------------------")
}

const (
	KB = 1024
	MB = 1024 * KB
)

// formatSize
func formatSize[T float64 | uint64](size T) string {
	switch {
	case size < KB:
		return fmt.Sprintf("%.0fB", float64(size))
	case size < MB:
		return fmt.Sprintf("%.1fKB", float64(size)/KB)
	default:
		return fmt.Sprintf("%.1fMB", float64(size)/MB)
	}
}
