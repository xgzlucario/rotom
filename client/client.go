package main

import (
	"fmt"
	"net"
	"time"

	"github.com/sourcegraph/conc/pool"
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

			for j := 0; j < 10000/50; j++ {
				// 发送请求
				_, err := conn.Write([]byte("ping"))
				if err != nil {
					fmt.Println("发送请求失败:", err)
					return
				}

				// 接收响应
				buf := make([]byte, 1024)
				size, err := conn.Read(buf)
				if size == 0 || err != nil {
					fmt.Println("接收响应失败:", err)
					return
				}
			}
		})
	}
	p.Wait()

	fmt.Println("10000 requests cost:", time.Since(start))
}
