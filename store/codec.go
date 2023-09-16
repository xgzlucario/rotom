package store

import (
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"unsafe"

	"github.com/xgzlucario/rotom/base"
)

const (
	_true  = 'T'
	_false = 'F'
)

var codecPool = sync.Pool{
	New: func() any {
		return &Codec{buf: make([]byte, 0, 8)}
	},
}

// Codec is the primary type for encoding data into a specific format.
type Codec struct {
	buf []byte
}

// NewCodec
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
	return s.format(s2b(&v))
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
	return s.format(base.FormatInt(v))
}

func (s *Codec) Int(v int64) *Codec {
	return s.format(base.FormatInt(v))
}

func (s *Codec) Float(f float64) *Codec {
	return s.format(strconv.AppendFloat(nil, f, 'f', -1, 64))
}

// format encodes a byte slice into the Coder's buffer as a record.
func (s *Codec) format(v []byte) *Codec {
	s.buf = append(s.buf, base.FormatInt(len(v))...)
	s.buf = append(s.buf, SEP_CHAR)
	s.buf = append(s.buf, v...)
	return s
}

func (s *Codec) Any(v any) (*Codec, error) {
	buf, err := s.encode(v)
	if err != nil {
		return nil, err
	}
	s.format(buf)
	return s, nil
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

// String convert to bytes unsafe
func s2b(str *string) []byte {
	strHeader := (*[2]uintptr)(unsafe.Pointer(str))
	byteSliceHeader := [3]uintptr{
		strHeader[0], strHeader[1], strHeader[1],
	}
	return *(*[]byte)(unsafe.Pointer(&byteSliceHeader))
}

// Bytes convert to string unsafe
func b2s(buf []byte) *string {
	return (*string)(unsafe.Pointer(&buf))
}
