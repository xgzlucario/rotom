package codeman

import (
	"encoding/binary"
	"math"

	cache "github.com/xgzlucario/GigaCache"
	"golang.org/x/exp/constraints"
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

type Integer constraints.Integer

// NewCodec
func NewCodec() *Codec {
	return &Codec{b: codecPool.Get(32)[:0]}
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

func (s *Codec) Uint32(v uint32) *Codec {
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
	s.b = append(s.b, formatStrSlice(v)...)
	return s
}

func (s *Codec) Uint32Slice(v []uint32) *Codec {
	s.b = append(s.b, formatNumberSlice(v)...)
	return s
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
