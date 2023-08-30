package main

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/xgzlucario/rotom/store"
)

func command() {
	start := time.Now()

	for i := 0; i < 10000; i++ {
		conn, err := net.Dial("tcp", ":7676")
		if err != nil {
			log.Fatal("Error connecting:", err)
		}

		k := strconv.Itoa(i)

		_, err = conn.Write(store.NewEncoder(store.OpSetTx, 4).
			Type(store.RecordString).String(k).Int(time.Now().Add(time.Second * 5).UnixNano()).
			String(k).Content())

		if err != nil {
			log.Fatal(err)
		}
		conn.Close()
	}
	fmt.Println("command", time.Since(start))
}

func bulkCommand() {
	start := time.Now()

	// 仅创建一次连接
	conn, err := net.Dial("tcp", ":7676")
	if err != nil {
		log.Fatal("Error connecting:", err)
	}
	defer conn.Close() // 确保连接最终会被关闭

	for i := 0; i < 10000; i++ {
		k := strconv.Itoa(i)

		_, err = conn.Write(store.NewEncoder(store.OpSetTx, 4).
			Type(store.RecordString).String(k).Int(time.Now().Add(time.Second * 5).UnixNano()).
			String(k).Content())

		if err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("bulk", time.Since(start))
}

func main() {
	command()
	bulkCommand()
}
