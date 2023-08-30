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

var encoderPool = sync.Pool{
	New: func() any {
		return &Encoder{buf: make([]byte, 0)}
	},
}

// Encoder is the primary type for encoding data into a specific format.
type Encoder struct {
	buf []byte
}

func NewEncoder(v Operation, argsNum byte) *Encoder {
	obj := encoderPool.Get().(*Encoder)
	obj.buf = append(obj.buf, byte(v), argsNum)
	return obj
}

func (s *Encoder) recycle() {
	s.buf = s.buf[:0]
	encoderPool.Put(s)
}

func (s *Encoder) String(v string) *Encoder {
	return s.format(base.S2B(&v))
}

func (s *Encoder) Type(v RecordType) *Encoder {
	return s.format([]byte{byte(v)})
}

func (s *Encoder) Bytes(v []byte) *Encoder {
	return s.format(v)
}

func (s *Encoder) Bool(v bool) *Encoder {
	if v {
		return s.format([]byte{_true})
	}
	return s.format([]byte{_false})
}

func (s *Encoder) Uint(v uint32) *Encoder {
	return s.format(base.FormatNumber(v))
}

func (s *Encoder) Int(v int64) *Encoder {
	return s.format(base.FormatNumber(v))
}

func (s *Encoder) length(v int) {
	s.buf = append(s.buf, base.FormatNumber(v)...)
	s.buf = append(s.buf, SEP_CHAR)
}

// format encodes a byte slice into the Coder's buffer as a record.
func (s *Encoder) format(v []byte) *Encoder {
	s.length(len(v))
	s.buf = append(s.buf, v...)
	s.buf = append(s.buf, SEP_CHAR)
	return s
}

func (s *Encoder) Any(v any) (*Encoder, error) {
	buf, err := s.encode(v)
	s.format(buf)
	return s, err
}

func (s *Encoder) Content() []byte {
	return s.buf
}

func (s *Encoder) encode(v any) ([]byte, error) {
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
