package store

import (
	"io"

	"github.com/bytedance/sonic"
	"github.com/panjf2000/gnet/v2"
	cache "github.com/xgzlucario/GigaCache"
	"github.com/xgzlucario/rotom/base"
)

// Response code inplements.
type RespCode int

const (
	RES_SUCCESS RespCode = iota + 1
	RES_ERROR
	RES_TIMEOUT
)

type RotomEngine struct {
	db *Store
	gnet.BuiltinEventEngine
}

type Resp struct {
	Data []byte   `json:"data"`
	Msg  []byte   `json:"msg"`
	Code RespCode `json:"code"`
}

// OnTraffic
func (e *RotomEngine) OnTraffic(conn gnet.Conn) gnet.Action {
	buf, err := io.ReadAll(conn)
	if err != nil {
		return gnet.Close
	}

	// handle event
	msg, err := e.db.handleEvent(buf)
	var resp Resp
	if err != nil {
		resp = Resp{Data: nil, Msg: []byte(err.Error()), Code: RES_ERROR}
	} else {
		resp = Resp{Data: msg, Msg: nil, Code: RES_SUCCESS}
	}

	data, err := sonic.Marshal(resp)
	if err != nil {
		return gnet.Close
	}

	// send resp
	_, err = conn.Write(data)
	if err != nil {
		return gnet.Close
	}

	return gnet.None
}

// handleEvent
func (db *Store) handleEvent(line []byte) (msg []byte, err error) {
	op := Operation(line[0])
	argsNum := int(line[1])

	// parse args by operation
	args, _, err := parseLine(line[2:], argsNum)
	if err != nil {
		return nil, err
	}

	switch op {
	case ReqPing:
		return []byte("pong"), nil

	case ReqLen:
		stat := db.Stat()
		return cache.FormatNumber(stat.Len), nil

	case ReqGet:
		v, _, ok := db.Get(*base.B2S(args[0]))
		if ok {
			return v, nil
		}

	case OpSetTx: // type, key, ts, val
		recType := VType(args[0][0])

		switch recType {
		case V_STRING:
			ts := cache.ParseNumber[int64](args[2])
			db.SetTx(*base.B2S(args[1]), args[3], ts)
		}

	default:
		return nil, base.ErrUnknownOperationType
	}

	return []byte("ok"), nil
}
