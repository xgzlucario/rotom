package main

import (
	"bytes"
	"fmt"
	"strconv"
	"time"

	"github.com/sourcegraph/conc/pool"
	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/store"
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

	validator := store.NewCodec(store.Response).Int(int64(store.RES_SUCCESS)).String("ok").B
	delays := make([]float64, 0, DATA_NUM)

	for i := 0; i < CLIENT_NUM; i++ {
		p.Go(func() {
			cli, err := store.NewClient(":7676")
			if err != nil {
				panic(err)
			}
			defer cli.Close()

			for j := 0; j < DATA_NUM/CLIENT_NUM; j++ {
				now := time.Now()
				addnow := now.Add(time.Second).UnixNano()
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
				delays = append(delays, float64(time.Since(now)))
			}
		})
	}
	p.Wait()

	fmt.Printf("%d requests cost: %v\n", DATA_NUM, time.Since(start))
	fmt.Printf("[qps] %.2f req/sec\n", DATA_NUM/time.Since(start).Seconds())

	// P99
	Sort(delays)
	fmt.Printf("[latency] avg: %v | min: %v | p50: %v | p95: %v | p99: %v | max: %v\n",
		time.Duration(Avg(delays)),
		time.Duration(Min(delays)),
		time.Duration(CalculatePercentile(delays, 50)),
		time.Duration(CalculatePercentile(delays, 95)),
		time.Duration(CalculatePercentile(delays, 99)),
		time.Duration(Max(delays)))

	fmt.Println()
}
