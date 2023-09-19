package main

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/sourcegraph/conc/pool"
	"github.com/xgzlucario/rotom/store"
)

const (
	DATA_NUM   = 100 * 10000
	CLIENT_NUM = 200
)

var (
	bpool = sync.Pool{
		New: func() any {
			return bytes.NewBuffer(make([]byte, 1024))
		},
	}
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
			conn, err := net.Dial("tcp", ":7676")
			if err != nil {
				panic(err)
			}
			defer conn.Close()

			for j := 0; j < DATA_NUM/CLIENT_NUM; j++ {
				k := strconv.Itoa(i)
				now := time.Now()

				cd := store.NewCodec(store.OpSetTx, 4).
					Type(store.TypeString).String(k).
					Int(now.Add(time.Minute).UnixNano()).String(k)

				// Write your logic here.
				send(conn, cd.Content(), func(res []byte) error {
					// fmt.Println(string(res))
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

	buf := bpool.Get().(*bytes.Buffer)
	defer bpool.Put(buf)

	n, err := conn.Read(buf.Bytes())
	if err != nil {
		return err
	}

	return callback(buf.Bytes()[:n])
}
