package store

import (
	"fmt"
	"io"
	"log"
	"net"

	"github.com/xgzlucario/rotom/base"
)

func (db *Store) Listen() error {
	addr := fmt.Sprintf("%s:%d", db.ListenIP, db.ListenPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	if db.Logger != nil {
		db.Logger.Info(fmt.Sprintf("listening on %s...", addr))
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go db.handleEvent(conn)
	}
}

func (db *Store) handleEvent(conn net.Conn) {
	defer conn.Close()

	line, err := io.ReadAll(conn)
	if err != nil {
		log.Println("Error reading:", err)
		return
	}

	var args [][]byte

	for len(line) > 2 {
		op := Operation(line[0])
		argsNum := int(line[1])
		line = line[2:]

		// parse args by operation
		args, line, err = parseLine(line, argsNum)
		if err != nil {
			panic(err)
		}

		switch op {
		case OpSetTx: // type, key, ts, val
			recType := RecordType(args[0][0])

			switch recType {
			case RecordString:
				ts := base.ParseNumber[int64](args[2])
				db.SetTx(*base.B2S(args[1]), args[2], ts)
			}
		}
	}
}
