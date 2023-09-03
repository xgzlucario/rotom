package main

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/bytedance/sonic"
	"github.com/sourcegraph/conc/pool"
	"github.com/xgzlucario/rotom/store"
)

const (
	DATA_NUM   = 100 * 10000
	CLIENT_NUM = 200
)

func main() {
	cmd()
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
					Type(store.V_STRING).String(k).
					Int(now.Add(time.Minute).UnixNano()).String(k)

				res, err := getAndRead(conn, cd.Content())
				if err != nil {
					panic(err)
				}
				if !bytes.Equal(res.Data, []byte("ok")) {
					panic("resp not except")
				}
			}
		})
	}
	p.Wait()

	fmt.Printf("%d requests cost: %v\n", DATA_NUM, time.Since(start))
	fmt.Printf("qps: %.2f req/sec\n", DATA_NUM/time.Since(start).Seconds())
}

func getAndRead(conn net.Conn, content []byte) (*store.Resp, error) {
	_, err := conn.Write(content)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}

	var res store.Resp
	if err := sonic.Unmarshal(buf[:n], &res); err != nil {
		fmt.Println(string(buf[:n]))
		panic(err)
	}

	return &res, nil
}
