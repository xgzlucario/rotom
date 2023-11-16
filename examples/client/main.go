package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/influxdata/tdigest"
	"github.com/sourcegraph/conc/pool"
	"github.com/xgzlucario/rotom"
)

const (
	DATA_NUM   = 100 * 10000
	CLIENT_NUM = 200
)

var (
	tdlock sync.Mutex
	td     = tdigest.NewWithCompression(1000)
)

func main() {
	for {
		cmd()
	}
}

func cmd() {
	start := time.Now()
	p := pool.New()

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
				err := cli.SetTx(k, []byte(k), addnow)
				if err != nil {
					panic(err)
				}

				// stat
				if j%100 == 0 {
					cost := time.Since(now)

					tdlock.Lock()
					td.Add(float64(cost), 1)
					tdlock.Unlock()
				}
			}
		})
	}
	p.Wait()

	// QPS
	fmt.Printf("%d requests cost: %v\n", DATA_NUM, time.Since(start))
	fmt.Printf("[qps] %.2f req/sec\n", DATA_NUM/time.Since(start).Seconds())

	// P99
	tdlock.Lock()
	fmt.Printf("[latency] p90: %v | p95: %v | p99: %v | p100: %v\n",
		time.Duration(td.Quantile(0.9)),
		time.Duration(td.Quantile(0.95)),
		time.Duration(td.Quantile(0.99)),
		time.Duration(td.Quantile(0.9999)))
	tdlock.Unlock()
	fmt.Println()
}
