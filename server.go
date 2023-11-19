package rotom

import (
	"bytes"
	"sync"

	"github.com/panjf2000/gnet/v2"
	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/codeman"
)

var (
	bufferpool = sync.Pool{
		New: func() any {
			return &base.CWriter{
				Buffer: bytes.NewBuffer(make([]byte, 0, 16)),
			}
		},
	}
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
	res, err := e.db.handleEvent(buf)
	var cd *codeman.Codec
	if err != nil {
		cd = NewCodec(Response).Int(RES_ERROR).Str(err.Error())

	} else {
		cd = NewCodec(Response).Int(RES_SUCCESS).Bytes(res.Bytes())
	}

	// send resp
	_, err = conn.Write(cd.Content())
	cd.Recycle()
	if res != nil {
		res.Reset()
		bufferpool.Put(res)
	}
	if err != nil {
		return gnet.Close
	}

	return gnet.None
}

// handleEvent
func (e *Engine) handleEvent(line []byte) (*base.CWriter, error) {
	decoder := codeman.NewDecoder(line)
	op, args, err := ParseRecord(decoder)
	if err != nil {
		return nil, err
	}

	buf := bufferpool.Get().(*base.CWriter)
	if err := cmdTable[op].hook(e, args, buf); err != nil {
		return nil, err
	}

	return buf, nil
}
