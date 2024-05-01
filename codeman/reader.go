package codeman

import (
	"encoding/binary"
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

func (s *Reader) readUvarint() uint64 {
	num, i := binary.Uvarint(s.b)
	if i == 0 {
		panic("codeman/bug: reader is done")
	}
	s.b = s.b[i:]
	return num
}

func (s *Reader) readVarint() int64 {
	num, i := binary.Varint(s.b)
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
	data := make([]string, s.readUvarint())
	for i := range data {
		data[i] = s.Str()
	}
	return data
}

func (s *Reader) Uint32Slice() []uint32 {
	data := make([]uint32, s.readUvarint())
	for i := range data {
		data[i] = s.Uint32()
	}
	return data
}

func (s *Reader) Uint32() uint32 {
	return uint32(s.readUvarint())
}

func (s *Reader) Int64() int64 {
	return int64(s.readVarint())
}

func (s *Reader) Bool() bool {
	return s.readUvarint() == _true
}

func (s *Reader) Byte() byte {
	return byte(s.readUvarint())
}
