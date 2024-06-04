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

	ValueOK = Value{typ: STRING, str: "OK"}

	ValueNull = Value{typ: NULL}
)

// Value represents the different types of RESP (Redis Serialization Protocol) values.
type Value struct {
	typ   byte    // Type of value ('string', 'error', 'integer', 'bulk', 'array', 'null')
	str   string  // Used for string and error types
	num   int64   // Used for integer type
	bulk  []byte  // Used for bulk strings
	array []Value // Used for arrays of nested values
}

// Resp is a parser for RESP encoded data.
type Resp struct {
	b []byte
}

// NewResp creates a new Resp object with a buffered reader.
func NewResp(b []byte) *Resp {
	return &Resp{b: b}
}

func newErrValue(err error) Value {
	return Value{typ: ERROR, str: err.Error()}
}

func newBulkValue(bulk []byte) Value {
	if bulk == nil {
		return Value{typ: NULL}
	}
	return Value{typ: BULK, bulk: bulk}
}

func newIntegerValue(n int) Value {
	return Value{typ: INTEGER, num: int64(n)}
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
		return Value{typ: INTEGER, num: int64(len)}, err
	default:
		return Value{}, fmt.Errorf("%w: %c", ErrUnknownType, _type)
	}
}

// readArray reads an array prefixed with '*' from the stream.
func (r *Resp) readArray() (Value, error) {
	v := Value{typ: ARRAY}

	len, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	v.array = make([]Value, len)
	for i := 0; i < len; i++ {
		val, err := r.Read()
		if err != nil {
			return v, err
		}
		v.array[i] = val
	}

	return v, nil
}

// readBulk reads a bulk string prefixed with '$' from the stream.
func (r *Resp) readBulk() (Value, error) {
	v := Value{typ: BULK}

	len, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	if len == -1 { // RESP Bulk strings can be null, indicated by "$-1"
		return Value{typ: NULL}, nil
	}

	v.bulk = r.b[:len]
	r.b = r.b[len:]

	r.readLine() // Read the trailing CRLF

	return v, nil
}

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
	str := strconv.FormatInt(v.num, 10)
	return append([]byte{INTEGER}, append([]byte(str), '\r', '\n')...)
}

// marshalString marshals a string value into RESP format.
func (v Value) marshalString() []byte {
	return append([]byte{STRING}, append([]byte(v.str), '\r', '\n')...)
}

// marshalBulk marshals a bulk string into RESP format.
func (v Value) marshalBulk() []byte {
	bulkHeader := append([]byte{BULK}, append([]byte(strconv.Itoa(len(v.bulk))), '\r', '\n')...)
	return append(bulkHeader, append(v.bulk, '\r', '\n')...)
}

// marshalArray marshals an array of values into RESP format.
func (v Value) marshalArray() []byte {
	arrayHeader := append([]byte{ARRAY}, append([]byte(strconv.Itoa(len(v.array))), '\r', '\n')...)
	for _, val := range v.array {
		arrayHeader = append(arrayHeader, val.Marshal()...)
	}
	return arrayHeader
}

// marshallError marshals an error message into RESP format.
func (v Value) marshallError() []byte {
	return append([]byte{ERROR}, append([]byte(v.str), '\r', '\n')...)
}

// marshallNull marshals a null value into RESP bulk string format.
func (v Value) marshallNull() []byte {
	return []byte("$-1\r\n")
}
