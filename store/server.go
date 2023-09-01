package store

import (
	"fmt"
	"io"

	"github.com/bytedance/sonic"
	"github.com/panjf2000/gnet/v2"
	"github.com/xgzlucario/rotom/base"
)

var (
	RESP_OK   = []byte("ok")
	RESP_PONG = []byte("pong")
)

type RotomEngine struct {
	db *Store
	gnet.BuiltinEventEngine
}

type Resp struct {
	Data  []byte `json:"d"`
	Error error  `json:"e"`
}

// OnTraffic
func (e *RotomEngine) OnTraffic(conn gnet.Conn) gnet.Action {
	buf, err := io.ReadAll(conn)
	if err != nil {
		return gnet.Close
	}

	// handle event
	msg, err := e.db.handleEvent(buf)
	data, err := sonic.Marshal(&Resp{Data: msg, Error: err})
	if err != nil {
		return gnet.Close
	}

	// send response
	_, err = conn.Write(data)
	if err != nil {
		return gnet.Close
	}

	return gnet.None
}

// Listen
func (db *Store) Listen() {
	addr := fmt.Sprintf("tcp://%s:%d", db.ListenIP, db.ListenPort)

	if db.Logger != nil {
		db.Logger.Info(fmt.Sprintf("listening on %s...", addr))
	}

	err := gnet.Run(&RotomEngine{db: db}, addr, gnet.WithMulticore(true))
	if err != nil {
		panic(err)
	}
}

// handleEvent
func (db *Store) handleEvent(line []byte) (msg []byte, err error) {
	var args [][]byte

	for len(line) > 2 {
		op := Operation(line[0])
		argsNum := int(line[1])
		line = line[2:]

		// parse args by operation
		args, line, err = parseLine(line, argsNum)
		if err != nil {
			return nil, err
		}

		switch op {
		case ReqPing:
			return RESP_PONG, nil

		case ReqLen:
			stat := db.Stat()
			return base.FormatNumber(stat.Len), nil

			// TODO
		case ReqHLen:

			// TODO
		case ReqLLen:

		case OpSetTx: // type, key, ts, val
			recType := RecordType(args[0][0])

			switch recType {
			case RecordString:
				ts := base.ParseNumber[int64](args[2])
				db.SetTx(*base.B2S(args[1]), args[3], ts)
			}

		default:
			return nil, base.ErrUnknownOperationType
		}
	}

	return RESP_OK, nil
}
