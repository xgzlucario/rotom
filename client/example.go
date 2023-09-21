package main

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/sourcegraph/conc/pool"
	cache "github.com/xgzlucario/GigaCache"
	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/store"
)

const (
	DATA_NUM   = 100 * 10000
	CLIENT_NUM = 200
)

var (
	bpool = cache.NewBytePoolCap(1000, 1024, 1024)
)

func main() {
	for {
		cmd()
	}
}

func cmd() {
	start := time.Now()
	p := pool.New()

	validContent := store.NewCodec(store.Response, 2).
		Int(int64(store.RES_SUCCESS)).String("ok").Content()

	for i := 0; i < CLIENT_NUM; i++ {
		p.Go(func() {
			conn, err := net.Dial("tcp", ":7676")
			if err != nil {
				panic(err)
			}
			defer conn.Close()

			for j := 0; j < DATA_NUM/CLIENT_NUM; j++ {
				now := time.Now()
				k := strconv.FormatInt(now.UnixNano(), 36)

				cd := store.NewCodec(store.OpSetTx, 4).
					Type(store.TypeString).String(k).
					Int(now.Add(time.Minute).UnixNano()).String(k)

				// Write your logic here.
				send(conn, cd.Content(), func(res []byte) error {
					if !bytes.Equal(res, validContent) {
						panic(base.ErrInvalidResponse)
					}
					return nil
				})

				cd.Recycle()
			}
		})
	}
	p.Wait()

	fmt.Printf("%d requests cost: %v\n", DATA_NUM, time.Since(start))
	fmt.Printf("qps: %.2f req/sec\n", DATA_NUM/time.Since(start).Seconds())
}

// send post request and handle response.
func send(conn net.Conn, req []byte, callback func([]byte) error) error {
	_, err := conn.Write(req)
	if err != nil {
		return err
	}

	buf := bpool.Get()
	defer bpool.Put(buf)

	n, err := conn.Read(buf)
	if err != nil {
		return err
	}

	return callback(buf[:n])
}
