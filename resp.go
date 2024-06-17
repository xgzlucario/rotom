package main

import (
	"fmt"
	"io"
	"slices"
	"strconv"
	"unsafe"
)

const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
	NULL    = 0xff
)

var (
	CRLF = []byte("\r\n")

	ValueOK = Value{typ: STRING, raw: []byte("OK")}

	ValueNull = Value{typ: NULL}
)

// Value represents the different types of RESP (Redis Serialization Protocol) values.
type Value struct {
	typ   byte    // Type of value ('string', 'error', 'integer', 'bulk', 'array', 'null')
	raw   []byte  // Used for string, error, integer and bulk strings
	array []Value // Used for arrays of nested values
}

// Arg represents the raw arguments bytes of RESP.
type Arg []byte

// Resp is a parser for RESP encoded data.
type Resp struct {
	b []byte
}

// NewResp creates a new Resp object with a buffered reader.
// DO NOT EDIT the `input` param because it will be referenced during read.
func NewResp(input []byte) *Resp {
	return &Resp{b: input}
}

func newErrValue(err error) Value {
	return Value{typ: ERROR, raw: []byte(err.Error())}
}

func newBulkValue(bulk []byte) Value {
	if bulk == nil {
		return Value{typ: NULL}
	}
	return Value{typ: BULK, raw: bulk}
}

func newIntegerValue(n int) Value {
	format := strconv.Itoa(n)
	return Value{typ: INTEGER, raw: []byte(format)}
}

func newArrayValue(value []Value) Value {
	return Value{typ: ARRAY, array: value}
}

func cutByCRLF(buf []byte) (before, after []byte, found bool) {
	if len(buf) <= 2 {
		return
	}
	for i, b := range buf {
		if b == '\r' {
			if buf[i+1] == '\n' {
				return buf[:i], buf[i+2:], true
			}
		}
	}
	return
}

func (r *Resp) ReadNextCommand(argsBuf []Arg) (args []Arg, err error) {
	if len(r.b) == 0 {
		return nil, io.EOF
	}
	args = argsBuf[:0]

	switch r.b[0] {
	case ARRAY:
		// command_bulk format
		before, after, ok := cutByCRLF(r.b[1:])
		if !ok {
			return nil, ErrCRLFNotFound
		}
		count, err := strconv.Atoi(b2s(before))
		if err != nil {
			return nil, err
		}
		r.b = after

		for i := 0; i < count; i++ {
			switch r.b[0] {
			case BULK:
			default:
				return nil, fmt.Errorf("unsupport array-in type: '%c'", r.b[0])
			}

			// read CRLF
			before, after, ok := cutByCRLF(r.b[1:])
			if !ok {
				return nil, ErrCRLFNotFound
			}
			count, err := strconv.Atoi(b2s(before))
			if err != nil {
				return nil, err
			}
			r.b = after

			args = append(args, r.b[:count])
			r.b = r.b[count+2:]
		}

	default:
		// command_inline format
		before, after, ok := cutByCRLF(r.b)
		if !ok {
			return nil, ErrUnknownCommand(string(r.b))
		}
		args = append(args, before)
		r.b = after
	}
	return
}

func (a Arg) ToString() string {
	return string(a)
}

func (a Arg) ToStringUnsafe() string {
	return b2s(a)
}

func (a Arg) ToInt() (int, error) {
	return strconv.Atoi(b2s(a))
}

func (a Arg) ToBytes() []byte {
	return a
}

func (a Arg) Clone() []byte {
	return slices.Clone(a)
}

func b2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// Append converts a Value object into its corresponding RESP bytes.
func (v Value) Append(b []byte) []byte {
	switch v.typ {
	case ARRAY:
		return v.appendArray(b)
	case BULK:
		return v.appendBulk(b)
	case STRING:
		return v.appendString(b)
	case INTEGER:
		return v.appendInteger(b)
	case NULL:
		return v.appendNull(b)
	case ERROR:
		return v.appendError(b)
	default:
		return append(b, ErrUnknownType.Error()...)
	}
}

// appendInteger appends a integer value into RESP format.
func (v Value) appendInteger(b []byte) []byte {
	b = append(b, INTEGER)
	b = append(b, v.raw...)
	b = append(b, CRLF...)
	return b
}

// appendString appends a string value into RESP format.
func (v Value) appendString(b []byte) []byte {
	b = append(b, STRING)
	b = append(b, v.raw...)
	b = append(b, CRLF...)
	return b
}

// appendBulk appends a bulk string into RESP format.
func (v Value) appendBulk(b []byte) []byte {
	format := strconv.Itoa(len(v.raw))
	b = append(b, BULK)
	b = append(b, format...)
	b = append(b, CRLF...)
	b = append(b, v.raw...)
	b = append(b, CRLF...)
	return b
}

// appendArray appends an array of values into RESP format.
func (v Value) appendArray(b []byte) []byte {
	b = append(b, ARRAY)
	b = append(b, strconv.Itoa(len(v.array))...)
	b = append(b, CRLF...)
	for _, val := range v.array {
		b = val.Append(b)
	}
	return b
}

// appendError appends an error message into RESP format.
func (v Value) appendError(b []byte) []byte {
	b = append(b, ERROR)
	b = append(b, v.raw...)
	b = append(b, CRLF...)
	return b
}

// appendNull appends a null value into RESP bulk string format.
func (v Value) appendNull(b []byte) []byte {
	return append(b, "$-1\r\n"...)
}
