package main

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/bytedance/sonic"
	"github.com/xgzlucario/rotom/store"
)

func main() {
	start := time.Now()

	for i := 0; i < 10000; i++ {
		conn, err := net.Dial("unix", "../rotom.socket")
		if err != nil {
			log.Fatal("Error connecting:", err)
		}

		data, err := sonic.Marshal(&store.Request{
			OP:      store.OpSetTx,
			RecType: store.RecordString,
			Args:    []string{"key", "value"},
		})
		if err != nil {
			log.Fatal(err)
		}

		_, err = conn.Write(data)
		if err != nil {
			log.Fatal(err)
		}

		conn.Close()
	}

	fmt.Println(time.Since(start))
}
