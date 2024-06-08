package main

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
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
	raw   []byte  // Used for string, error, integer ad bulk strings
	array []Value // Used for arrays of nested values
}

// Resp is a parser for RESP encoded data.
// It is a ZERO-COPY parser.
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

// readLine reads a line ending with CRLF from the reader.
func (r *Resp) readLine() ([]byte, int, error) {
	before, after, found := bytes.Cut(r.b, CRLF)
	if found {
		r.b = after
		return before, len(before) + 2, nil
	}
	return nil, 0, ErrCRLFNotFound
}

// readInteger reads an integer value following the ':' prefix.
func (r *Resp) readInteger() (x int, n int, err error) {
	line, n, err := r.readLine()
	if err != nil {
		return 0, 0, err
	}
	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err
	}
	return int(i64), n, nil
}

func (r *Resp) readByte() (byte, error) {
	if len(r.b) == 0 {
		return 0, io.EOF
	}
	b := r.b[0]
	r.b = r.b[1:]
	return b, nil
}

// Read parses the next RESP value from the stream.
func (r *Resp) Read() (Value, error) {
	_type, err := r.readByte()
	if err != nil {
		return Value{}, err
	}

	switch _type {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	case INTEGER:
		len, _, err := r.readInteger()
		if err != nil {
			return Value{}, err
		} else {
			return newIntegerValue(len), nil
		}
	default:
		return Value{}, fmt.Errorf("%w: %c", ErrUnknownType, _type)
	}
}

// readArray reads an array prefixed with '*' from the stream.
func (r *Resp) readArray() (Value, error) {
	value := Value{typ: ARRAY}

	n, _, err := r.readInteger()
	if err != nil {
		return Value{}, err
	}

	value.array = make([]Value, n)
	for i := range value.array {
		v, err := r.Read()
		if err != nil {
			return Value{}, err
		}
		value.array[i] = v
	}

	return value, nil
}

// readBulk reads a bulk string prefixed with '$' from the stream.
func (r *Resp) readBulk() (Value, error) {
	value := Value{typ: BULK}

	n, _, err := r.readInteger()
	if err != nil {
		return Value{}, err
	}

	if n == -1 { // RESP Bulk strings can be null, indicated by "$-1"
		return Value{typ: NULL}, err
	}

	value.raw = r.b[:n]
	r.b = r.b[n:]

	r.readLine() // Read the trailing CRLF

	return value, nil
}

func (v Value) ToString() string { return string(v.raw) }

func (v Value) ToBytes() []byte { return v.raw }

// Marshal converts a Value object into its corresponding RESP bytes.
func (v Value) Marshal() []byte {
	switch v.typ {
	case ARRAY:
		return v.marshalArray()
	case BULK:
		return v.marshalBulk()
	case STRING:
		return v.marshalString()
	case INTEGER:
		return v.marshalInteger()
	case NULL:
		return v.marshallNull()
	case ERROR:
		return v.marshallError()
	default:
		return []byte(ErrUnknownType.Error())
	}
}

func (v Value) marshalInteger() []byte {
	w := bytes.NewBuffer(nil)
	w.WriteByte(INTEGER)
	w.Write(v.raw)
	w.Write(CRLF)
	return w.Bytes()
}

// marshalString marshals a string value into RESP format.
func (v Value) marshalString() []byte {
	w := bytes.NewBuffer(nil)
	w.WriteByte(STRING)
	w.Write(v.raw)
	w.Write(CRLF)
	return w.Bytes()
}

// marshalBulk marshals a bulk string into RESP format.
func (v Value) marshalBulk() []byte {
	w := bytes.NewBuffer(nil)
	w.WriteByte(BULK)
	w.WriteString(strconv.Itoa(len(v.raw)))
	w.Write(CRLF)
	w.Write(v.raw)
	w.Write(CRLF)
	return w.Bytes()
}

// marshalArray marshals an array of values into RESP format.
func (v Value) marshalArray() []byte {
	w := bytes.NewBuffer(nil)
	w.WriteByte(ARRAY)
	w.WriteString(strconv.Itoa(len(v.array)))
	w.Write(CRLF)
	for _, val := range v.array {
		w.Write(val.Marshal())
	}
	return w.Bytes()
}

// marshallError marshals an error message into RESP format.
func (v Value) marshallError() []byte {
	w := bytes.NewBuffer(nil)
	w.WriteByte(ERROR)
	w.Write(v.raw)
	w.Write(CRLF)
	return w.Bytes()
}

// marshallNull marshals a null value into RESP bulk string format.
func (v Value) marshallNull() []byte {
	return []byte("$-1\r\n")
}
