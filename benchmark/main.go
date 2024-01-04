package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/influxdata/tdigest"
	"github.com/xgzlucario/rotom"
)

const (
	KB = 1 << (10 * (iota + 1))
	MB
	GB
)

func convertSize(size int64) string {
	switch {
	case size >= GB:
		return fmt.Sprintf("%.1fGB", float64(size)/GB)
	case size >= MB:
		return fmt.Sprintf("%.1fMB", float64(size)/MB)
	case size >= KB:
		return fmt.Sprintf("%.1fKB", float64(size)/KB)
	default:
		return fmt.Sprintf("%dB", size)
	}
}

func fileSize(filename string) string {
	fi, err := os.Stat(filename)
	if err != nil {
		return ""
	}
	size := fi.Size()
	return convertSize(size)
}

func createDB() *rotom.DB {
	options := rotom.DefaultOptions
	options.Logger = nil
	options.DirPath = fmt.Sprintf("%d", time.Now().UnixNano())

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

	start := time.Now()
	td := tdigest.NewWithCompression(1000)

	db := createDB()

	for i := 0; i < 100*10000; i++ {
		t1 := time.Now()

		k := fmt.Sprintf("%010d", i)
		db.Set(k, []byte(k))

		if i%10 == 0 {
			td.Add(float64(time.Since(t1)), 1)
		}
	}

	fmt.Println("cost:", time.Since(start))
	fmt.Printf("50th: %.0f ns\n", td.Quantile(0.5))
	fmt.Printf("90th: %.0f ns\n", td.Quantile(0.9))
	fmt.Printf("99th: %.0f ns\n", td.Quantile(0.99))
	// wait for sync
	time.Sleep(time.Second)
	fmt.Printf("db file size: %v\n", fileSize(db.DirPath))
	fmt.Println()
}

func benchSet8parallel() {
	fmt.Println("========== Set 8 parallel ==========")
	fmt.Println("size: 100*10000 enties")
	fmt.Println("desc: key 10 bytes, value 10 bytes")

	start := time.Now()
	td := tdigest.NewWithCompression(1000)

	db := createDB()

	var wg sync.WaitGroup
	wg.Add(8)

	for i := 0; i < 8; i++ {
		i := i
		go func() {
			start := i * 100 * 10000
			for n := 0; n < 100*10000/8; n++ {
				t1 := time.Now()
				k := fmt.Sprintf("%010d", start+n)
				db.Set(k, []byte(k))

				if i == 0 && n%10 == 0 {
					td.Add(float64(time.Since(t1)), 1)
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()

	fmt.Println("cost:", time.Since(start))
	fmt.Printf("50th: %.0f ns\n", td.Quantile(0.5))
	fmt.Printf("90th: %.0f ns\n", td.Quantile(0.9))
	fmt.Printf("99th: %.0f ns\n", td.Quantile(0.99))
	// wait for sync
	time.Sleep(time.Second)
	fmt.Printf("db file size: %v\n", fileSize(db.DirPath))
	fmt.Println()
}

func benchSetEx() {
	fmt.Println("========== SetEx ==========")
	fmt.Println("size: 100*10000 enties")
	fmt.Println("desc: key 10 bytes, value 10 bytes, ttl 1min")

	start := time.Now()
	td := tdigest.NewWithCompression(1000)

	db := createDB()

	for i := 0; i < 100*10000; i++ {
		t1 := time.Now()

		k := fmt.Sprintf("%010d", i)
		db.SetEx(k, []byte(k), time.Minute)

		if i%10 == 0 {
			td.Add(float64(time.Since(t1)), 1)
		}
	}

	fmt.Println("cost:", time.Since(start))
	fmt.Printf("50th: %.0f ns\n", td.Quantile(0.5))
	fmt.Printf("90th: %.0f ns\n", td.Quantile(0.9))
	fmt.Printf("99th: %.0f ns\n", td.Quantile(0.99))
	// wait for sync
	time.Sleep(time.Second)
	fmt.Printf("db file size: %v\n", fileSize(db.DirPath))
	fmt.Println()
}

func benchGet() {
	fmt.Println("========== Get ==========")
	fmt.Println("size: 100*10000 enties")
	fmt.Println("desc: key 10 bytes, value 10 bytes")

	td := tdigest.NewWithCompression(1000)

	db := createDB()

	for i := 0; i < 100*10000; i++ {
		k := fmt.Sprintf("%010d", i)
		db.Set(k, []byte(k))
	}

	start := time.Now()

	for i := 0; i < 100*10000; i++ {
		t1 := time.Now()
		db.Get(fmt.Sprintf("%010d", i))

		if i%10 == 0 {
			td.Add(float64(time.Since(t1)), 1)
		}
	}

	fmt.Println("cost:", time.Since(start))
	fmt.Printf("50th: %.0f ns\n", td.Quantile(0.5))
	fmt.Printf("90th: %.0f ns\n", td.Quantile(0.9))
	fmt.Printf("99th: %.0f ns\n", td.Quantile(0.99))
	fmt.Println()
}

func benchHSet() {
	fmt.Println("========== HSet ==========")
	fmt.Println("size: 100*10000 enties")
	fmt.Println("desc: field 10 bytes, value 10 bytes")

	start := time.Now()
	td := tdigest.NewWithCompression(1000)

	db := createDB()

	for i := 0; i < 100*10000; i++ {
		t1 := time.Now()

		k := fmt.Sprintf("%010d", i)
		db.HSet("mymap", k, []byte(k))

		if i%10 == 0 {
			td.Add(float64(time.Since(t1)), 1)
		}
	}

	fmt.Println("cost:", time.Since(start))
	fmt.Printf("50th: %.0f ns\n", td.Quantile(0.5))
	fmt.Printf("90th: %.0f ns\n", td.Quantile(0.9))
	fmt.Printf("99th: %.0f ns\n", td.Quantile(0.99))
	// wait for sync
	time.Sleep(time.Second)
	fmt.Printf("db file size: %v\n", fileSize(db.DirPath))
	fmt.Println()
}

func benchLRPush() {
	fmt.Println("========== LRPush ==========")
	fmt.Println("size: 100*10000 enties")
	fmt.Println("desc: value 10 bytes")

	start := time.Now()
	td := tdigest.NewWithCompression(1000)

	db := createDB()

	for i := 0; i < 100*10000; i++ {
		t1 := time.Now()

		k := fmt.Sprintf("%010d", i)
		db.LRPush("mylist", k)

		if i%10 == 0 {
			td.Add(float64(time.Since(t1)), 1)
		}
	}

	fmt.Println("cost:", time.Since(start))
	fmt.Printf("50th: %.0f ns\n", td.Quantile(0.5))
	fmt.Printf("90th: %.0f ns\n", td.Quantile(0.9))
	fmt.Printf("99th: %.0f ns\n", td.Quantile(0.99))
	// wait for sync
	time.Sleep(time.Second)
	fmt.Printf("db file size: %v\n", fileSize(db.DirPath))
	fmt.Println()
}

func benchHGet() {
	fmt.Println("========== HGet ==========")
	fmt.Println("size: 100*10000 enties")
	fmt.Println("desc: field 10 bytes, value 10 bytes")

	td := tdigest.NewWithCompression(1000)

	db := createDB()

	for i := 0; i < 100*10000; i++ {
		k := fmt.Sprintf("%010d", i)
		db.HSet("mymap", k, []byte(k))
	}

	start := time.Now()

	for i := 0; i < 100*10000; i++ {
		t1 := time.Now()
		db.HGet("mymap", fmt.Sprintf("%010d", i))

		if i%10 == 0 {
			td.Add(float64(time.Since(t1)), 1)
		}
	}

	fmt.Println("cost:", time.Since(start))
	fmt.Printf("50th: %.0f ns\n", td.Quantile(0.5))
	fmt.Printf("90th: %.0f ns\n", td.Quantile(0.9))
	fmt.Printf("99th: %.0f ns\n", td.Quantile(0.99))
	fmt.Println()
}

func benchBitSet() {
	fmt.Println("========== BitSet ==========")
	fmt.Println("size: 100*10000 enties")
	fmt.Println("desc: offset uint32")

	start := time.Now()
	td := tdigest.NewWithCompression(1000)

	db := createDB()

	for i := 0; i < 100*10000; i++ {
		t1 := time.Now()

		db.BitSet("bm", true, uint32(i))

		if i%10 == 0 {
			td.Add(float64(time.Since(t1)), 1)
		}
	}

	fmt.Println("cost:", time.Since(start))
	fmt.Printf("50th: %.0f ns\n", td.Quantile(0.5))
	fmt.Printf("90th: %.0f ns\n", td.Quantile(0.9))
	fmt.Printf("99th: %.0f ns\n", td.Quantile(0.99))
	// wait for sync
	time.Sleep(time.Second)
	fmt.Printf("db file size: %v\n", fileSize(db.DirPath))
	fmt.Println()
}

func benchGet8parallel() {
	fmt.Println("========== Get 8 parallel ==========")
	fmt.Println("size: 100*10000 enties")
	fmt.Println("desc: key 10 bytes, value 10 bytes")

	td := tdigest.NewWithCompression(1000)

	db := createDB()

	for i := 0; i < 100*10000; i++ {
		k := fmt.Sprintf("%010d", i)
		db.Set(k, []byte(k))
	}

	start := time.Now()

	var wg sync.WaitGroup
	wg.Add(8)

	for i := 0; i < 8; i++ {
		i := i
		go func() {
			start := i * 100 * 10000
			for n := 0; n < 100*10000/8; n++ {
				t1 := time.Now()
				db.Get(fmt.Sprintf("%010d", start+n))

				if i == 0 && n%10 == 0 {
					td.Add(float64(time.Since(t1)), 1)
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()

	fmt.Println("cost:", time.Since(start))
	fmt.Printf("50th: %.0f ns\n", td.Quantile(0.5))
	fmt.Printf("90th: %.0f ns\n", td.Quantile(0.9))
	fmt.Printf("99th: %.0f ns\n", td.Quantile(0.99))
	fmt.Println()
}

func main() {
	benchSet()
	benchSet8parallel()
	benchSetEx()
	benchGet()
	benchGet8parallel()
	benchLRPush()
	benchHSet()
	benchHGet()
	benchBitSet()
}
