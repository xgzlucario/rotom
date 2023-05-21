package store

import (
	"fmt"
	"reflect"
	"strconv"
	"sync"

	"github.com/bytedance/sonic"
	"github.com/xgzlucario/rotom/base"
)

// _base is the base for integer conversion.
const (
	_base = 36
)

// coderPool is a pool of Coder objects to improve performance by reusing Coder instances.
var coderPool = sync.Pool{
	New: func() any { return new(Coder) },
}

// Coder is the primary type for encoding data into a specific format.
type Coder struct {
	buf []byte
	err error
}

func NewCoder(v Operation) *Coder {
	obj := coderPool.Get().(*Coder)
	obj.buf = append(obj.buf, byte(v))
	return obj
}

func putCoder(obj *Coder) {
	obj.buf = obj.buf[:0]
	obj.err = nil
	coderPool.Put(obj)
}

func (s *Coder) String(v string) *Coder {
	s.int(len(v))
	s.buf = append(s.buf, ':')
	s.buf = append(s.buf, v...)
	s.buf = append(s.buf, recordSepChar)
	return s
}

func (s *Coder) Type(v RecordType) *Coder {
	s.buf = append(s.buf, byte(v))
	return s
}

func (s *Coder) Bytes(v []byte) *Coder {
	s.int(len(v))
	s.buf = append(s.buf, ':')
	s.buf = append(s.buf, v...)
	s.buf = append(s.buf, recordSepChar)
	return s
}

func (s *Coder) int(v int) *Coder {
	str := strconv.FormatInt(int64(v), _base)
	s.buf = append(s.buf, str...)
	return s
}

// format encodes a byte slice into the Coder's buffer as a record.
func (s *Coder) format(v []byte) *Coder {
	s.int(len(v))
	s.buf = append(s.buf, ':')
	s.buf = append(s.buf, v...)
	s.buf = append(s.buf, recordSepChar)
	return s
}

func (s *Coder) Bool(v bool) *Coder {
	if v {
		return s.format([]byte{'T'})
	} else {
		return s.format([]byte{'F'})
	}
}

func (s *Coder) Uint(v uint) *Coder {
	str := strconv.FormatUint(uint64(v), _base)
	return s.format(base.S2B(&str))
}

func (s *Coder) Int64(v int64) *Coder {
	str := strconv.FormatInt(v, _base)
	return s.format(base.S2B(&str))
}

func (s *Coder) Any(v any) *Coder {
	buf, err := s.encode(v)
	if err != nil {
		s.err = err
	}
	s.format(buf)
	return s
}

func (s *Coder) encode(v any) ([]byte, error) {
	switch v := v.(type) {
	case String:
		return v, nil
	case Map:
		return sonic.Marshal(v)
	case Set:
		return sonic.Marshal(v)
	case List:
		return v.MarshalJSON()
	case ZSet:
		return v.MarshalJSON()
	case BitMap:
		return v.MarshalJSON()
	default:
		panic(fmt.Errorf("%v: %v", base.ErrUnSupportDataType, reflect.TypeOf(v).String()))
	}
}
