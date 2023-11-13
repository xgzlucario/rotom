package rotom

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"strconv"
	"sync"

	"github.com/xgzlucario/rotom/base"
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
	return s.formatString(v)
}

func (s *Codec) StrSlice(v []string) *Codec {
	return s.format(formatStrSlice(v))
}

func (s *Codec) Bytes(v []byte) *Codec {
	return s.format(v)
}

func (s *Codec) Bool(v bool) *Codec {
	return s.formatByte(formatBool(v))
}

func (s *Codec) Uint(v uint32) *Codec {
	return s.format(formatVarint(nil, v))
}

func (s *Codec) Int(v int64) *Codec {
	return s.format(formatVarint(nil, v))
}

func (s *Codec) Float(f float64) *Codec {
	return s.format(strconv.AppendFloat(nil, f, 'f', -1, 64))
}

// format uses variable-length encoding of incoming bytes.
func (s *Codec) format(v []byte) *Codec {
	s.B = formatVarint(s.B, len(v))
	s.B = append(s.B, v...)
	return s
}

// formatByte uses variable-length encoding of incoming byte.
func (s *Codec) formatByte(v byte) *Codec {
	s.B = formatVarint(s.B, 1)
	s.B = append(s.B, v)
	return s
}

// formatString uses variable-length encoding of incoming string.
func (s *Codec) formatString(v string) *Codec {
	s.B = formatVarint(s.B, len(v))
	s.B = append(s.B, v...)
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

func (s *Codec) encode(v any) ([]byte, error) {
	switch v := v.(type) {
	case base.Binarier:
		return v.MarshalBinary()
	case base.Jsoner:
		return v.MarshalJSON()
	default:
		return nil, fmt.Errorf("%w: %v", base.ErrUnSupportDataType, reflect.TypeOf(v))
	}
}

type Decoder struct {
	b []byte
}

type Result []byte

func (r Result) ToStr() string {
	return string(r)
}

func (r Result) ToBool() bool {
	return r[0] == _true
}

func (r Result) ToInt64() int64 {
	return int64(parseVarint(r))
}

func (r Result) ToInt() int {
	return int(parseVarint(r))
}

func (r Result) ToUint32() uint32 {
	return uint32(parseVarint(r))
}

func (r Result) ToUint64() uint64 {
	return parseVarint(r)
}

func (r Result) ToStrSlice() []string {
	length, n := binary.Uvarint(r)
	r = r[n:]
	data := make([]string, 0, length)
	for i := uint64(0); i < length; i++ {
		klen, n := binary.Uvarint(r)
		r = r[n:]
		data = append(data, string(r[:klen]))
		r = r[klen:]
	}
	return data
}

func (r Result) ToUint32Slice() []uint32 {
	length, n := binary.Uvarint(r)
	r = r[n:]
	data := make([]uint32, 0, length)
	for i := uint64(0); i < length; i++ {
		k, n := binary.Uvarint(r)
		r = r[n:]
		data = append(data, uint32(k))
	}
	return data
}

func NewDecoder(buf []byte) *Decoder {
	return &Decoder{b: buf}
}

// ParseRecord parse one operation record line.
func (s *Decoder) ParseRecord() (op Operation, res []Result, err error) {
	if s.Done() {
		return 0, nil, base.ErrParseRecordLine
	}
	op = Operation(s.b[0])
	line := s.b[1:]

	// bound check.
	if int(op) >= len(cmdTable) {
		return 0, nil, base.ErrParseRecordLine
	}

	argsNum := cmdTable[op].argsNum
	res = make([]Result, 0, argsNum)

	// parses args.
	for j := 0; j < int(argsNum); j++ {
		num, i := binary.Uvarint(line)
		if i == 0 {
			return 0, nil, base.ErrParseRecordLine
		}
		klen := int(num)

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

// formatInt
func formatVarint[T base.Integer](buf []byte, n T) []byte {
	if buf == nil {
		buf = make([]byte, 0, binary.MaxVarintLen64)
	}
	return binary.AppendUvarint(buf, uint64(n))
}

// parseInt
func parseVarint(b []byte) uint64 {
	n, _ := binary.Uvarint(b)
	return n
}

// formatStrSlice
func formatStrSlice(s []string) []byte {
	data := make([]byte, 0, len(s)*2+1)
	data = binary.AppendUvarint(data, uint64(len(s)))
	for _, v := range s {
		data = binary.AppendUvarint(data, uint64(len(v)))
		data = append(data, v...)
	}
	return data
}

// formatU32Slice
func formatU32Slice(s []uint32) []byte {
	data := make([]byte, 0, len(s)+1)
	data = binary.AppendUvarint(data, uint64(len(s)))
	for _, v := range s {
		data = binary.AppendUvarint(data, uint64(v))
	}
	return data
}

// formatBool
func formatBool(b bool) byte {
	if b {
		return _true
	}
	return _false
}
