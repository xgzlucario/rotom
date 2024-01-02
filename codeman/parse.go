package codeman

import (
	"encoding/binary"
	"errors"
)

var (
	ErrParserIsDone      = errors.New("rotom/codeman: parser is done")
	ErrParseData         = errors.New("rotom/codeman: parse data error")
	ErrUnSupportDataType = errors.New("rotom/codeman: unsupport data type")
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
func (s *Parser) Parse() (res anyResult) {
	if s.Error != nil {
		return nil
	}
	res, s.Error = s.parse()
	return
}

// ParseVarint
func (s *Parser) ParseVarint() (num varintResult) {
	if s.Error != nil {
		return 0
	}
	num, s.Error = s.parseVarint()
	return
}

// Parse parses a record from Parser.
func (s *Parser) parse() (anyResult, error) {
	if s.Done() {
		return nil, ErrParserIsDone
	}

	// parses varint length.
	num, i := binary.Uvarint(s.b)
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
func (s *Parser) parseVarint() (varintResult, error) {
	if s.Done() {
		return 0, ErrParserIsDone
	}

	// parses varint.
	num, i := binary.Uvarint(s.b)
	s.b = s.b[i:]

	return varintResult(num), nil
}

func (s *Parser) Done() bool {
	return len(s.b) == 0
}
