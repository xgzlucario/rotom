package rotom

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"strconv"
	"sync"

	"github.com/xgzlucario/rotom/base"
	bproto "github.com/xgzlucario/rotom/proto"
	"google.golang.org/protobuf/proto"
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
	return s.format([]byte(v))
}

func (s *Codec) StrSlice(v []string) *Codec {
	return s.format(formatStrSlice(v))
}

func (s *Codec) Type(v Type) *Codec {
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
	return s.format(formatVarint(nil, v))
}

func (s *Codec) Int(v int64) *Codec {
	return s.format(formatVarint(nil, v))
}

func (s *Codec) Float(f float64) *Codec {
	return s.format(strconv.AppendFloat(nil, f, 'f', -1, 64))
}

// format encodes a byte slice into the Coder's buffer as a record.
func (s *Codec) format(v []byte) *Codec {
	// encode length.
	s.B = formatVarint(s.B, len(v))
	// encode data.
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

func (r Result) ToByte() byte {
	return r[0]
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
	dst := &bproto.StrSlice{}
	proto.Unmarshal(r, dst)
	return dst.Data
}

func (r Result) ToUint32Slice() []uint32 {
	dst := &bproto.UintSlice{}
	proto.Unmarshal(r, dst)
	return dst.Data
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
func formatStrSlice(ss []string) []byte {
	src, _ := proto.Marshal(&bproto.StrSlice{Data: ss})
	return src
}

// formatU32Slice
func formatU32Slice(ss []uint32) []byte {
	src, _ := proto.Marshal(&bproto.UintSlice{Data: ss})
	return src
}

// formatBool
func formatBool(b bool) byte {
	if b {
		return _true
	}
	return _false
}
