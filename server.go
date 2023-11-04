package rotom

import (
	"bytes"

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
func (e *Engine) handleEvent(line []byte) ([]byte, error) {
	op, args, err := NewDecoder(line).ParseRecord()
	if err != nil {
		return nil, err
	}

	res := &base.CWriter{Buffer: bytes.NewBuffer(nil)}
	if err := cmdTable[op].hook(e, args, res); err != nil {
		return nil, err
	}

	return res.Bytes(), nil
}
