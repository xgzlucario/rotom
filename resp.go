package main

import (
	"bufio"
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

type Resp struct {
	reader *bufio.Reader
}

// NewResp creates a new Resp object with a buffered reader.
func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}

// readLine reads a line ending with CRLF from the reader.
func (r *Resp) readLine() (line []byte, n int, err error) {
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		n += 1
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' && line[len(line)-1] == '\n' {
			break
		}
	}
	return line[:len(line)-2], n, nil // Trim the CRLF at the end
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

// Read parses the next RESP value from the stream.
func (r *Resp) Read() (Value, error) {
	_type, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch _type {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	case INTEGER:
		n, _, err := r.readInteger()
		if err != nil {
			return Value{}, err
		} else {
			return newIntegerValue(n), nil
		}
	default:
		return Value{}, fmt.Errorf("unknown value type %v", _type)
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

	bulk := make([]byte, len)
	_, err = io.ReadFull(r.reader, bulk) // Use ReadFull to ensure we read exactly 'len' bytes
	if err != nil {
		return v, err
	}
	v.bulk = bulk

	r.readLine() // Read the trailing CRLF

	return v, nil
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
	w.WriteString(strconv.FormatInt(v.num, 10))
	w.Write(CRLF)
	return w.Bytes()
}

// marshalString marshals a string value into RESP format.
func (v Value) marshalString() []byte {
	w := bytes.NewBuffer(nil)
	w.WriteByte(STRING)
	w.WriteString(v.str)
	w.Write(CRLF)
	return w.Bytes()
}

// marshalBulk marshals a bulk string into RESP format.
func (v Value) marshalBulk() []byte {
	w := bytes.NewBuffer(nil)
	w.WriteByte(BULK)
	w.WriteString(strconv.Itoa(len(v.bulk)))
	w.Write(CRLF)
	w.Write(v.bulk)
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
	w.WriteString(v.str)
	w.Write(CRLF)
	return w.Bytes()
}

// marshallNull marshals a null value into RESP bulk string format.
func (v Value) marshallNull() []byte {
	return []byte("$-1\r\n")
}
