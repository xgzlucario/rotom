package codeman

import (
	"encoding/binary"
	"errors"
	"sync"
)

var (
	ErrDecoderIsDone = errors.New("decoder is done")
	ErrParseData     = errors.New("parse data error")

	decoderPool = sync.Pool{
		New: func() any { return &Decoder{} },
	}
)

type Decoder struct {
	b []byte
}

func NewDecoder(buf []byte) *Decoder {
	decoder := decoderPool.Get().(*Decoder)
	decoder.b = buf
	return decoder
}

// Parse parse a specified length of data.
func (s *Decoder) Parse(length int) (r []Result, err error) {
	if s.Done() {
		return nil, ErrDecoderIsDone
	}
	r = make([]Result, 0, length)

	// parses args.
	for j := 0; j < int(length); j++ {
		num, i := binary.Uvarint(s.b)
		if i == 0 {
			return nil, ErrParseData
		}
		klen := int(num)

		// bound check.
		if i+klen > len(s.b) {
			return nil, ErrParseData
		}
		r = append(r, s.b[i:i+klen])
		s.b = s.b[i+klen:]
	}

	return
}

// Parse parse a specified length of data.
func (s *Decoder) ParseOne() (Result, error) {
	r, err := s.Parse(1)
	if err != nil {
		return nil, err
	}
	return r[0], nil
}

func (s *Decoder) Done() bool {
	return len(s.b) == 0
}

func (s *Decoder) Recycle(obj *Decoder) {
	obj.b = nil
	decoderPool.Put(obj)
}
