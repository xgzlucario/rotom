package codeman

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"

	cache "github.com/xgzlucario/GigaCache"
)

const (
	_true  = 1
	_false = 0
)

var codecPool = cache.NewBufferPool()

// Codec is the primary type for encoding data into a specific format.
type Codec struct {
	b []byte
}

// NewCodec
func NewCodec() *Codec {
	return &Codec{b: codecPool.Get(16)[:0]}
}

func (s *Codec) Recycle() {
	codecPool.Put(s.b)
}

func (s *Codec) Content() []byte {
	return s.b
}

func (s *Codec) Str(v string) *Codec {
	return s.formatString(v)
}

func (s *Codec) Byte(v byte) *Codec {
	s.b = append(s.b, v)
	return s
}

func (s *Codec) Bytes(v []byte) *Codec {
	return s.format(v)
}

func (s *Codec) Bool(v bool) *Codec {
	if v {
		s.b = append(s.b, _true)
	} else {
		s.b = append(s.b, _false)
	}
	return s
}

func (s *Codec) Uint(v uint32) *Codec {
	s.b = formatVarint(s.b, v)
	return s
}

func (s *Codec) Int(v int64) *Codec {
	s.b = formatVarint(s.b, v)
	return s
}

func (s *Codec) Float(f float64) *Codec {
	s.b = formatVarint(s.b, math.Float64bits(f))
	return s
}

func (s *Codec) StrSlice(v []string) *Codec {
	return s.format(formatStrSlice(v))
}

func (s *Codec) Uint32Slice(v []uint32) *Codec {
	return s.format(formatNumberSlice(v))
}

// format uses variable-length encoding of incoming bytes.
func (s *Codec) format(v []byte) *Codec {
	s.b = formatVarint(s.b, len(v))
	s.b = append(s.b, v...)
	return s
}

// formatString uses variable-length encoding of incoming string.
func (s *Codec) formatString(v string) *Codec {
	s.b = formatVarint(s.b, len(v))
	s.b = append(s.b, v...)
	return s
}

// Any encodes any type of data.
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
	case Binarier:
		return v.MarshalBinary()
	case Jsoner:
		return v.MarshalJSON()
	default:
		return nil, fmt.Errorf("%w: %v", ErrUnSupportDataType, reflect.TypeOf(v))
	}
}

func formatVarint[T Integer](buf []byte, n T) []byte {
	return binary.AppendUvarint(buf, uint64(n))
}

func formatStrSlice(s []string) []byte {
	data := make([]byte, 0, len(s)*2+1)
	data = binary.AppendUvarint(data, uint64(len(s)))
	for _, v := range s {
		data = binary.AppendUvarint(data, uint64(len(v)))
		data = append(data, v...)
	}
	return data
}

func formatNumberSlice[T Integer](s []T) []byte {
	data := make([]byte, 0, len(s)+1)
	data = binary.AppendUvarint(data, uint64(len(s)))
	for _, v := range s {
		data = binary.AppendUvarint(data, uint64(v))
	}
	return data
}
