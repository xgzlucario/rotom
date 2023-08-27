package store

import (
	"io"
	"log"
	"net"
	"os"

	"github.com/bytedance/sonic"
)

var count uint64

type Request struct {
	OP      Operation  `json:"O"`
	RecType RecordType `json:"R"`
	Args    []string   `json:"A"`
}

func (db *Store) Listen() error {
	if err := os.Remove("rotom.socket"); err != nil && !os.IsNotExist(err) {
		log.Fatal(err)
	}

	listener, err := net.Listen("unix", "rotom.socket")
	if err != nil {
		return err
	}
	defer listener.Close()

	if db.Logger != nil {
		db.Logger.Info("listening on unix socket...")
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go handleEvent(conn)
	}
}

func handleEvent(conn net.Conn) {
	defer conn.Close()

	buf, err := io.ReadAll(conn)
	if err != nil {
		log.Println("Error reading:", err)
		return
	}

	var req Request
	if err := sonic.Unmarshal(buf, &req); err != nil {
		panic(err)
	}

	count++
	log.Println("Received data:", req, count)
}
