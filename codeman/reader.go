package codeman

import (
	"encoding/binary"
	"math"
)

type Reader struct {
	b []byte
}

func NewReader(buf []byte) *Reader {
	return &Reader{b: buf}
}

func (s *Reader) read() []byte {
	num, i := binary.Uvarint(s.b)
	if i == 0 {
		panic("codeman/bug: reader is done")
	}
	end := i + int(num)

	// bound check.
	_ = s.b[end-1]

	res := s.b[i:end]
	s.b = s.b[end:]

	return res
}

func (s *Reader) readVarint() uint64 {
	num, i := binary.Uvarint(s.b)
	if i == 0 {
		panic("codeman/bug: reader is done")
	}
	s.b = s.b[i:]

	return num
}

func (s *Reader) Done() bool {
	return len(s.b) == 0
}

func (s *Reader) RawBytes() []byte {
	return s.read()
}

func (s *Reader) Str() string {
	return string(s.read())
}

func (s *Reader) StrSlice() []string {
	r := s.read()
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

func (s *Reader) Uint32Slice() []uint32 {
	r := s.read()
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

func (s *Reader) Uint32() uint32 {
	r := s.readVarint()
	return uint32(r)
}

func (s *Reader) Int64() int64 {
	r := s.readVarint()
	return int64(r)
}

func (s *Reader) Bool() bool {
	r := s.readVarint()
	return r == _true
}

func (s *Reader) Byte() byte {
	r := s.readVarint()
	return byte(r)
}

func (s *Reader) Float64() float64 {
	r := s.readVarint()
	return math.Float64frombits(r)
}
