package main

import (
	"bytes"
	"fmt"
	"net"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/bytedance/sonic"
	"github.com/sourcegraph/conc/pool"
	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/store"
)

func main() {
	start := time.Now()

	p := pool.New().WithMaxGoroutines(50)
	for i := 0; i < 50; i++ {
		p.Go(func() {
			conn, err := net.Dial("tcp", ":7676")
			if err != nil {
				panic(err)
			}
			defer conn.Close()

			now := time.Now()

			for j := 0; j < 10000/50; j++ {
				num := gofakeit.Phone()
				cd := store.NewEncoder(store.OpSetTx, 4).
					Type(store.RecordString).String(num).
					Int(now.Add(time.Minute).UnixNano()).String("test")

				res, err := GetAndRead(conn, cd.Content())
				if err != nil {
					panic(err)
				}
				if !bytes.Equal(res.Data, store.RESP_OK) {
					panic("resp not except")
				}
			}

			// Get Length
			cd := store.NewEncoder(store.ReqLen, 1).String("")
			res, err := GetAndRead(conn, cd.Content())
			if err != nil {
				panic(err)
			}
			fmt.Println(res, base.ParseNumber[int](res.Data))
		})
	}
	p.Wait()

	fmt.Println("10000 requests cost:", time.Since(start))
	fmt.Printf("qps: %.2f req/sec\n", float64(10000)/time.Since(start).Seconds())
}

// GetAndRead
func GetAndRead(conn net.Conn, content []byte) (*store.Resp, error) {
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
		panic(err)
	}

	return &res, nil
}
