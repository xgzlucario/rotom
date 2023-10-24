package rotom

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"unsafe"

	"github.com/xgzlucario/rotom/base"
	"github.com/zeebo/xxh3"
)

const (
	_true  = 'T'
	_false = 'F'
)

var codecPool = sync.Pool{
	New: func() any { return &Codec{B: make([]byte, 0, 16)} },
}

// Codec is the primary type for encoding data into a specific format.
type Codec struct {
	B []byte
}

// NewCodec
func NewCodec(v Operation) *Codec {
	obj := codecPool.Get().(*Codec)
	obj.B = append(obj.B, byte(v))
	return obj
}

func (s *Codec) Recycle() {
	s.B = s.B[:0]
	codecPool.Put(s)
}

func (s *Codec) Str(v string) *Codec {
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
	s.B = append(s.B, base.FormatInt(len(v))...)
	s.B = append(s.B, SepChar)
	s.B = append(s.B, v...)
	return s
}

// crc
func (s *Codec) crc() {
	s.B = binary.LittleEndian.AppendUint32(s.B, uint32(xxh3.Hash(s.B)))
}

func (s *Codec) Any(v any) (*Codec, error) {
	buf, err := s.encode(v)
	if err != nil {
		return nil, err
	}
	s.format(buf)
	return s, nil
}

func (s *Codec) encode(v any) ([]byte, error) {
	switch v := v.(type) {
	case String:
		return v, nil
	case base.Binarier:
		return v.MarshalBinary()
	case base.Gober:
		return v.GobEncode()
	case base.Jsoner:
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

type Decoder struct {
	b []byte
}

func NewDecoder(buf []byte) *Decoder {
	return &Decoder{b: buf}
}

// ParseRecord parse one operation record line.
func (s *Decoder) ParseRecord() (op Operation, res [][]byte, err error) {
	if s.Done() {
		return 0, nil, base.ErrParseRecordLine
	}
	op = Operation(s.b[0])
	line := s.b[1:]

	// bound check.
	if int(op) >= len(cmdTable) {
		return 0, nil, base.ErrParseRecordLine
	}

	argsNum := cmdTable[op].ArgsNum
	res = make([][]byte, 0, argsNum)

	// parses args.
	for j := 0; j < argsNum; j++ {
		i := bytes.IndexByte(line, SepChar)
		if i <= 0 {
			return 0, nil, base.ErrParseRecordLine
		}

		klen := base.ParseInt[int](line[:i])
		i++

		// bound check.
		if i+klen > len(line) {
			return 0, nil, base.ErrParseRecordLine
		}
		res = append(res, line[i:i+klen])

		line = line[i+klen:]
	}
	s.b = line

	return
}

func (s *Decoder) Done() bool {
	return len(s.b) == 0
}
