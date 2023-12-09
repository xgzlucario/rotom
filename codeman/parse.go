package codeman

import (
	"encoding/binary"
	"errors"
)

var (
	ErrParserIsDone = errors.New("Parser is done")
	ErrParseData    = errors.New("parse data error")
)

type Parser struct {
	b     []byte
	Error error
}

// NewParser
func NewParser(buf []byte) *Parser {
	return &Parser{b: buf}
}

// Parse
func (s *Parser) Parse() (res AnyResult) {
	if s.Error != nil {
		return nil
	}
	res, s.Error = s.parse()
	return
}

// ParseVarint
func (s *Parser) ParseVarint() (num VarintResult) {
	if s.Error != nil {
		return 0
	}
	num, s.Error = s.parseVarint()
	return
}

// Parse parses a record from Parser.
func (s *Parser) parse() (AnyResult, error) {
	if s.Done() {
		return nil, ErrParserIsDone
	}

	// parses varint length.
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

// Parse parses a record from Parser.
func (s *Parser) parseVarint() (VarintResult, error) {
	if s.Done() {
		return 0, ErrParserIsDone
	}

	// parses varint.
	num, i := binary.Uvarint(s.b)
	if i == 0 {
		return 0, ErrParseData
	}
	s.b = s.b[i:]

	return VarintResult(num), nil
}

func (s *Parser) Done() bool {
	return len(s.b) == 0
}
