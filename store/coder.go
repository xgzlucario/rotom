package store

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/xgzlucario/rotom/base"
)

// Record like:
// <SetTx><key_len>SEP<key><ts>SEP<value_len>SEP<value>SEP
// SEP is seperator, now is byte(255)

const (
	_true  = 'T'
	_false = 'F'
)

// coderPool is a pool of Coder objects to improve performance by reusing Coder instances.
var coderPool = sync.Pool{
	New: func() any {
		return &Coder{buf: make([]byte, 0)}
	},
}

// Coder is the primary type for encoding data into a specific format.
type Coder struct {
	buf []byte
}

func NewCoder(v Operation) *Coder {
	obj := coderPool.Get().(*Coder)
	obj.buf = append(obj.buf, byte(v))
	return obj
}

func putCoder(obj *Coder) {
	obj.buf = obj.buf[:0]
	coderPool.Put(obj)
}

func (s *Coder) String(v string) *Coder {
	s.int(len(v))
	s.buf = append(s.buf, v...)
	s.buf = append(s.buf, SEP_CHAR)
	return s
}

func (s *Coder) Type(v RecordType) *Coder {
	s.buf = append(s.buf, byte(v))
	return s
}

func (s *Coder) Bytes(v []byte) *Coder {
	s.int(len(v))
	s.buf = append(s.buf, v...)
	s.buf = append(s.buf, SEP_CHAR)
	return s
}

func (s *Coder) int(v int) {
	s.buf = append(s.buf, base.FormatNumber(v)...)
	s.buf = append(s.buf, SEP_CHAR)
}

// format encodes a byte slice into the Coder's buffer as a record.
func (s *Coder) format(v []byte) *Coder {
	s.int(len(v))
	s.buf = append(s.buf, v...)
	s.buf = append(s.buf, SEP_CHAR)
	return s
}

func (s *Coder) Bool(v bool) *Coder {
	if v {
		return s.format([]byte{_true})
	}
	return s.format([]byte{_false})
}

func (s *Coder) Uint(v uint) *Coder {
	return s.format(base.FormatNumber(v))
}

func (s *Coder) Ts(v int64) *Coder {
	s.buf = append(s.buf, base.FormatNumber(v)...)
	s.buf = append(s.buf, SEP_CHAR)
	return s
}

func (s *Coder) Any(v any) (*Coder, error) {
	buf, err := s.encode(v)
	s.format(buf)
	return s, err
}

func (s *Coder) encode(v any) ([]byte, error) {
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
