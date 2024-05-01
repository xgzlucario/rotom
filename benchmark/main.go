package main

import (
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/xgzlucario/rotom"
)

const N = 100 * 10000

type Quantile struct {
	mu sync.Mutex
	f  []float64
}

func NewQuantile(size int) *Quantile {
	return &Quantile{f: make([]float64, 0, size)}
}

func (q *Quantile) Add(v float64) {
	q.f = append(q.f, v)
}

func (q *Quantile) SyncAdd(v float64) {
	q.mu.Lock()
	q.f = append(q.f, v)
	q.mu.Unlock()
}

func (q *Quantile) Quantile(p float64) float64 {
	return q.f[int(float64(len(q.f))*p)]
}

func (q *Quantile) Print() {
	slices.Sort(q.f)
	fmt.Printf("50th: %.0f ns\n", q.Quantile(0.5))
	fmt.Printf("90th: %.0f ns\n", q.Quantile(0.9))
	fmt.Printf("99th: %.0f ns\n", q.Quantile(0.99))
}

func createDB() *rotom.DB {
	options := rotom.DefaultOptions
	options.DirPath = fmt.Sprintf("tmp-%d", time.Now().UnixNano())

	db, err := rotom.Open(options)
	if err != nil {
		panic(err)
	}

	return db
}

func benchSet() {
	fmt.Println("========== Set ==========")
	fmt.Println("size: 100*10000 enties")
	fmt.Println("desc: key 10 bytes, value 10 bytes")

	quant := NewQuantile(N)
	db := createDB()
	start := time.Now()

	for i := 0; i < N; i++ {
		t1 := time.Now()
		k := fmt.Sprintf("%010d", i)
		db.Set(k, []byte(k))
		quant.Add(float64(time.Since(t1)))
	}

	fmt.Println("cost:", time.Since(start))
	fmt.Printf("qps: %.2f\n", float64(N)/time.Since(start).Seconds())
	quant.Print()
	fmt.Println()
}

func benchBatchSet() {
	fmt.Println("========== BatchSet ==========")
	fmt.Println("size: 100*10000 enties")
	fmt.Println("desc: key 10 bytes, value 10 bytes, 100 key-values a batch")

	quant := NewQuantile(N)
	db := createDB()
	start := time.Now()

	batches := make([]*rotom.Batch, 0, 100)
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("%010d", i)
		batches = append(batches, &rotom.Batch{
			Key: k,
			Val: []byte(k),
		})
		if len(batches) == 100 {
			t1 := time.Now()
			db.BatchSet(batches...)
			quant.Add(float64(time.Since(t1)))
			batches = batches[:0]
		}
	}

	fmt.Println("cost:", time.Since(start))
	fmt.Printf("qps: %.2f\n", float64(N)/time.Since(start).Seconds())
	quant.Print()
	fmt.Println()
}

func benchGet() {
	fmt.Println("========== Get ==========")
	fmt.Println("size: 100*10000 enties")
	fmt.Println("desc: key 10 bytes, value 10 bytes")

	quant := NewQuantile(N)
	db := createDB()

	for i := 0; i < N; i++ {
		k := fmt.Sprintf("%010d", i)
		db.Set(k, []byte(k))
	}

	start := time.Now()

	for i := 0; i < N; i++ {
		t1 := time.Now()
		db.Get(fmt.Sprintf("%010d", i))
		quant.Add(float64(time.Since(t1)))
	}

	fmt.Println("cost:", time.Since(start))
	fmt.Printf("qps: %.2f\n", float64(N)/time.Since(start).Seconds())
	quant.Print()
	fmt.Println()
}

func benchHSet() {
	fmt.Println("========== HSet ==========")
	fmt.Println("size: 100*10000 enties")
	fmt.Println("desc: field 10 bytes, value 10 bytes")

	quant := NewQuantile(N)
	db := createDB()
	start := time.Now()

	for i := 0; i < N; i++ {
		t1 := time.Now()
		k := fmt.Sprintf("%010d", i)
		db.HSet("mymap", k, []byte(k))
		quant.Add(float64(time.Since(t1)))
	}

	fmt.Println("cost:", time.Since(start))
	fmt.Printf("qps: %.2f\n", float64(N)/time.Since(start).Seconds())
	quant.Print()
	fmt.Println()
}

func benchBatchHSet() {
	fmt.Println("========== BatchHSet ==========")
	fmt.Println("size: 100*10000 enties")
	fmt.Println("desc: field 10 bytes, value 10 bytes, 100 key-values a batch")

	quant := NewQuantile(N)
	db := createDB()
	start := time.Now()

	batches := make([]*rotom.Batch, 0, 100)
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("%010d", i)
		batches = append(batches, &rotom.Batch{
			Key: k,
			Val: []byte(k),
		})
		if len(batches) == 100 {
			t1 := time.Now()
			db.BatchHSet("mymap", batches...)
			quant.Add(float64(time.Since(t1)))
			batches = batches[:0]
		}
	}

	fmt.Println("cost:", time.Since(start))
	fmt.Printf("qps: %.2f\n", float64(N)/time.Since(start).Seconds())
	quant.Print()
	fmt.Println()
}

func benchLRPush() {
	fmt.Println("========== RPush ==========")
	fmt.Println("size: 100*10000 enties")
	fmt.Println("desc: value 10 bytes")

	quant := NewQuantile(N)
	db := createDB()
	start := time.Now()

	for i := 0; i < N; i++ {
		t1 := time.Now()
		k := fmt.Sprintf("%010d", i)
		db.RPush("mylist", k)
		quant.Add(float64(time.Since(t1)))
	}

	fmt.Println("cost:", time.Since(start))
	fmt.Printf("qps: %.2f\n", float64(N)/time.Since(start).Seconds())
	quant.Print()
	fmt.Println()
}

func benchHGet() {
	fmt.Println("========== HGet ==========")
	fmt.Println("size: 100*10000 enties")
	fmt.Println("desc: field 10 bytes, value 10 bytes")

	quant := NewQuantile(N)
	db := createDB()

	for i := 0; i < N; i++ {
		k := fmt.Sprintf("%010d", i)
		db.HSet("mymap", k, []byte(k))
	}

	start := time.Now()

	for i := 0; i < N; i++ {
		t1 := time.Now()
		k := fmt.Sprintf("%010d", i)
		db.HGet("mymap", k)
		quant.Add(float64(time.Since(t1)))
	}

	fmt.Println("cost:", time.Since(start))
	fmt.Printf("qps: %.2f\n", float64(N)/time.Since(start).Seconds())
	quant.Print()
	fmt.Println()
}

func benchBitSet() {
	fmt.Println("========== BitSet ==========")
	fmt.Println("size: 100*10000 enties")
	fmt.Println("desc: offset uint32")

	quant := NewQuantile(N)
	db := createDB()
	start := time.Now()

	for i := 0; i < N; i++ {
		t1 := time.Now()
		db.BitSet("bm", true, uint32(i))
		quant.Add(float64(time.Since(t1)))
	}

	fmt.Println("cost:", time.Since(start))
	fmt.Printf("qps: %.2f\n", float64(N)/time.Since(start).Seconds())
	quant.Print()
	fmt.Println()
}

func benchZSet() {
	fmt.Println("========== ZSet ==========")
	fmt.Println("size: 100*10000 enties")
	fmt.Println("desc: field 10 bytes, incr int64")

	quant := NewQuantile(N)
	db := createDB()
	start := time.Now()

	for i := 0; i < N; i++ {
		t1 := time.Now()
		k := fmt.Sprintf("%010d", i)
		db.ZIncr("myzset", k, int64(i))
		quant.Add(float64(time.Since(t1)))
	}

	fmt.Println("cost:", time.Since(start))
	fmt.Printf("qps: %.2f\n", float64(N)/time.Since(start).Seconds())
	quant.Print()
	fmt.Println()
}

func benchGet8parallel() {
	fmt.Println("========== Get 8 parallel ==========")
	fmt.Println("size: 100*10000 enties")
	fmt.Println("desc: key 10 bytes, value 10 bytes")

	quant := NewQuantile(N)
	db := createDB()

	for i := 0; i < N; i++ {
		k := fmt.Sprintf("%010d", i)
		db.Set(k, []byte(k))
	}

	start := time.Now()

	var wg sync.WaitGroup
	wg.Add(8)

	for i := 0; i < 8; i++ {
		i := i
		go func() {
			start := i * N
			for n := 0; n < N/8; n++ {
				t1 := time.Now()
				db.Get(fmt.Sprintf("%010d", start+n))

				quant.SyncAdd(float64(time.Since(t1)))
			}
			wg.Done()
		}()
	}

	wg.Wait()

	fmt.Println("cost:", time.Since(start))
	fmt.Printf("qps: %.2f\n", float64(N)/time.Since(start).Seconds())
	quant.Print()
	fmt.Println()
}

func main() {
	benchSet()
	benchBatchSet()
	benchGet()
	benchGet8parallel()
	benchLRPush()
	benchHSet()
	benchBatchHSet()
	benchHGet()
	benchBitSet()
	benchZSet()
}
