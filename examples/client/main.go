package main

import (
	"bytes"
	"fmt"
	"strconv"
	"time"

	"github.com/sourcegraph/conc/pool"
	cache "github.com/xgzlucario/GigaCache"
	"github.com/xgzlucario/rotom"
	"github.com/xgzlucario/rotom/base"
)

const (
	DATA_NUM   = 100 * 10000
	CLIENT_NUM = 200
)

func main() {
	for {
		cmd()
	}
}

func cmd() {
	start := time.Now()
	p := pool.New()

	validator := rotom.NewCodec(rotom.Response).Int(int64(rotom.RES_SUCCESS)).Str("ok").B
	delays := cache.NewPercentile()

	for i := 0; i < CLIENT_NUM; i++ {
		p.Go(func() {
			cli, err := rotom.NewClient(":7676")
			if err != nil {
				panic(err)
			}
			defer cli.Close()

			for j := 0; j < DATA_NUM/CLIENT_NUM; j++ {
				now := time.Now()
				addnow := now.Add(time.Second * 5).UnixNano()
				k := strconv.FormatInt(addnow, 36)

				// send
				res, err := cli.SetTx(k, []byte(k), addnow)
				if err != nil {
					panic(err)
				}
				if !bytes.Equal(res, validator) {
					panic(base.ErrInvalidResponse)
				}

				// stat
				delays.Add(float64(time.Since(now)))
			}
		})
	}
	p.Wait()

	// QPS
	fmt.Printf("%d requests cost: %v\n", DATA_NUM, time.Since(start))
	fmt.Printf("[qps] %.2f req/sec\n", DATA_NUM/time.Since(start).Seconds())

	// P99
	fmt.Printf("[latency] avg: %v | min: %v | p50: %v | p95: %v | p99: %v | max: %v\n",
		time.Duration(delays.Avg()),
		time.Duration(delays.Min()),
		time.Duration(delays.Percentile(50)),
		time.Duration(delays.Percentile(90)),
		time.Duration(delays.Percentile(99)),
		time.Duration(delays.Max()))

	fmt.Println()
}
