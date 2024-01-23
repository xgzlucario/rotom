package codeman

import (
	"encoding/binary"
)

type Parser struct {
	b []byte
}

// NewParser
func NewParser(buf []byte) *Parser {
	return &Parser{b: buf}
}

func (s *Parser) Parse() anyResult {
	num, i := binary.Uvarint(s.b)
	if i == 0 {
		panic("codeman/bug: please check parse is done before")
	}
	end := i + int(num)

	// bound check.
	_ = s.b[end-1]

	res := s.b[i:end]
	s.b = s.b[end:]

	return res
}

func (s *Parser) ParseVarint() varintResult {
	num, i := binary.Uvarint(s.b)
	if i == 0 {
		panic("codeman/bug: please check parse is done before")
	}
	s.b = s.b[i:]

	return varintResult(num)
}

func (s *Parser) Done() bool {
	return len(s.b) == 0
}
