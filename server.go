package rotom

import (
	"io"

	"github.com/panjf2000/gnet/v2"
	"github.com/xgzlucario/rotom/base"
)

// Response code inplements.
type RespCode byte

const (
	RES_SUCCESS RespCode = iota + 1
	RES_ERROR
	RES_TIMEOUT
	RES_LIMITED
)

type RotomEngine struct {
	db *Store
	gnet.BuiltinEventEngine
}

// OnTraffic
func (e *RotomEngine) OnTraffic(conn gnet.Conn) gnet.Action {
	buf, err := io.ReadAll(conn)
	if err != nil {
		return gnet.Close
	}

	// handle event
	msg, err := e.db.handleEvent(buf)
	var cd *Codec
	if err != nil {
		cd = NewCodec(Response).Int(int64(RES_ERROR)).String(err.Error())

	} else {
		cd = NewCodec(Response).Int(int64(RES_SUCCESS)).Bytes(msg)
	}

	// send resp
	_, err = conn.Write(cd.B)
	cd.Recycle()
	if err != nil {
		return gnet.Close
	}

	return gnet.None
}

// handleEvent
func (db *Store) handleEvent(line []byte) (msg []byte, err error) {
	op := Operation(line[0])
	argsNum := cmdTable[op]

	// parse args by operation
	args, _, err := parseLine(line[1:], argsNum)
	if err != nil {
		return nil, err
	}

	switch op {
	case ReqPing:
		return []byte("pong"), nil

	case ReqLen:
		stat := db.Stat()
		return base.FormatInt(stat.Len), nil

	case ReqGet:
		v, _, ok := db.Get(*b2s(args[0]))
		if ok {
			return v.([]byte), nil
		}
		return nil, base.ErrKeyNotFound

	case OpSetTx: // type, key, ts, val
		recType := VType(args[0][0])

		switch recType {
		case TypeString:
			ts := base.ParseInt[int64](args[2])
			db.SetTx(*b2s(args[1]), args[3], ts)
		}

	case OpLPush: // key, item
		db.LPush(*b2s(args[0]), *b2s(args[1]))

	case OpRPush: // key, item
		db.RPush(*b2s(args[0]), *b2s(args[1]))

	case OpLPop: // key
		r, err := db.LPop(*b2s(args[0]))
		return s2b(&r), err

	case OpRPop: // key
		r, err := db.RPop(*b2s(args[0]))
		return s2b(&r), err

	case ReqLLen: // key
		num, err := db.LLen(*b2s(args[0]))
		return base.FormatInt(num), err

	default:
		return nil, base.ErrUnknownOperationType
	}

	return []byte("ok"), nil
}
