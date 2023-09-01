package store

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/xgzlucario/rotom/base"
)

const (
	_true  = 'T'
	_false = 'F'
)

var codecPool = sync.Pool{
	New: func() any {
		return &Codec{buf: make([]byte, 0)}
	},
}

// Codec is the primary type for encoding data into a specific format.
type Codec struct {
	buf []byte
}

func NewCodec(v Operation, argsNum byte) *Codec {
	obj := codecPool.Get().(*Codec)
	obj.buf = append(obj.buf, byte(v), argsNum)
	return obj
}

func (s *Codec) recycle() {
	s.buf = s.buf[:0]
	codecPool.Put(s)
}

func (s *Codec) String(v string) *Codec {
	return s.format(base.S2B(&v))
}

func (s *Codec) Type(v VType) *Codec {
	return s.format([]byte{byte(v)})
}

func (s *Codec) Bytes(v []byte) *Codec {
	return s.format(v)
}

func (s *Codec) Bool(v bool) *Codec {
	if v {
		return s.format([]byte{_true})
	}
	return s.format([]byte{_false})
}

func (s *Codec) Uint(v uint32) *Codec {
	return s.format(base.FormatNumber(v))
}

func (s *Codec) Int(v int64) *Codec {
	return s.format(base.FormatNumber(v))
}

// format encodes a byte slice into the Coder's buffer as a record.
func (s *Codec) format(v []byte) *Codec {
	s.buf = append(s.buf, base.FormatNumber(len(v))...)
	s.buf = append(s.buf, SEP_CHAR)
	s.buf = append(s.buf, v...)
	return s
}

func (s *Codec) Any(v any) (*Codec, error) {
	buf, err := s.encode(v)
	s.format(buf)
	return s, err
}

func (s *Codec) Content() []byte {
	return s.buf
}

func (s *Codec) encode(v any) ([]byte, error) {
	switch v := v.(type) {
	case String:
		return v, nil
	case base.Binarier:
		return v.MarshalBinary()
	case base.Gober:
		return v.GobEncode()
	case base.Marshaler:
		return v.MarshalJSON()
	default:
		return nil, fmt.Errorf("%v: %v", base.ErrUnSupportDataType, reflect.TypeOf(v))
	}
}
