package codeman

import (
	"encoding/binary"
	"errors"
)

var (
	ErrDecoderIsDone = errors.New("decoder is done")
	ErrParseData     = errors.New("parse data error")
)

type Decoder struct {
	b []byte
}

func NewDecoder(buf []byte) *Decoder {
	return &Decoder{b: buf}
}

// Parses multiple records of a specified length from a decoder..
func (s *Decoder) Parses(length int) (r []Result, err error) {
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

// Parse parses a record from decoder.
func (s *Decoder) Parse() (Result, error) {
	if s.Done() {
		return nil, ErrDecoderIsDone
	}
	// parses one arg.
	num, i := binary.Uvarint(s.b)
	if i == 0 {
		return nil, ErrParseData
	}
	klen := int(num)

	// bound check.
	if i+klen > len(s.b) {
		return nil, ErrParseData
	}

	res := s.b[i : i+klen]
	s.b = s.b[i+klen:]

	return res, nil
}

func (s *Decoder) Done() bool {
	return len(s.b) == 0
}
