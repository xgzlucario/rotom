package rotom

import (
	"github.com/panjf2000/gnet/v2"
	"github.com/xgzlucario/rotom/base"
)

// Response code inplements.
type RespCode = int64

const (
	RES_SUCCESS RespCode = iota + 1
	RES_ERROR
	RES_TIMEOUT
)

type RotomEngine struct {
	db *Engine
	gnet.BuiltinEventEngine
}

// OnTraffic
func (e *RotomEngine) OnTraffic(conn gnet.Conn) gnet.Action {
	buf, err := conn.Next(-1)
	if err != nil {
		return gnet.Close
	}

	// handle event
	msg, err := e.db.handleEvent(buf)
	var cd *Codec
	if err != nil {
		cd = NewCodec(Response).Int(RES_ERROR).Str(err.Error())

	} else {
		cd = NewCodec(Response).Int(RES_SUCCESS).Bytes(msg)
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
func (e *Engine) handleEvent(line []byte) (msg []byte, err error) {
	op, args, err := NewDecoder(line).ParseRecord()
	if err != nil {
		return nil, err
	}

	switch op {
	case ReqPing:
		return []byte("pong"), nil

	case ReqLen:
		stat := e.Stat()
		return base.FormatInt(stat.Len), nil

	case ReqGet:
		v, _, ok := e.Get(*b2s(args[0]))
		if ok {
			return v.([]byte), nil
		}
		return nil, base.ErrKeyNotFound

	case OpSetTx: // type, key, ts, val
		ts := base.ParseInt[int64](args[2])
		e.SetTx(*b2s(args[1]), args[3], ts)

	case OpRename: // new, old
		e.Rename(*b2s(args[0]), *b2s(args[1]))

	case OpRemove: // key
		e.Remove(*b2s(args[0]))

	case OpLPush: // key, item
		e.LPush(*b2s(args[0]), *b2s(args[1]))

	case OpRPush: // key, item
		e.RPush(*b2s(args[0]), *b2s(args[1]))

	case OpLPop: // key
		r, err := e.LPop(*b2s(args[0]))
		return s2b(&r), err

	case OpRPop: // key
		r, err := e.RPop(*b2s(args[0]))
		return s2b(&r), err

	case ReqLLen: // key
		num, err := e.LLen(*b2s(args[0]))
		return base.FormatInt(num), err

	default:
		return nil, base.ErrUnknownOperationType
	}

	return []byte("ok"), nil
}
