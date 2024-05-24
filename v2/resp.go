package main

import (
	"bufio"
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
)

type ValueType byte

const (
	TypeNull ValueType = iota
	TypeString
	TypeError
	TypeInteger
	TypeBulk
	TypeArray
)

// Value represents the different types of RESP (Redis Serialization Protocol) values.
type Value struct {
	typ   ValueType // Type of value ('string', 'error', 'integer', 'bulk', 'array')
	str   []byte    // Used for string and error types
	num   int64     // Used for integer type
	bulk  []byte    // Used for bulk strings
	array []Value   // Used for arrays of nested values
}

// Resp is a parser for RESP encoded data.
type Resp struct {
	reader *bufio.Reader
}

// NewResp creates a new Resp object with a buffered reader.
func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}

func ErrValue(desc string) Value {
	return Value{typ: TypeError, str: []byte(desc)}
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
	default:
		fmt.Printf("Unknown type: %v", string(_type))
		return Value{}, fmt.Errorf("unknown value type %v", _type)
	}
}

// readArray reads an array prefixed with '*' from the stream.
func (r *Resp) readArray() (Value, error) {
	v := Value{typ: TypeArray}

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
	v := Value{typ: TypeBulk}

	len, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	if len == -1 { // RESP Bulk strings can be null, indicated by "$-1"
		return Value{typ: TypeNull}, nil
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

// Marshal converts a Value object into its corresponding RESP bytes.
func (v Value) Marshal() []byte {
	switch v.typ {
	case TypeArray:
		return v.marshalArray()
	case TypeBulk:
		return v.marshalBulk()
	case TypeString:
		return v.marshalString()
	case TypeInteger:
		return v.marshalInteger()
	case TypeNull:
		return v.marshallNull()
	case TypeError:
		return v.marshallError()
	default:
		return []byte("unknown type")
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
	return append([]byte{ERROR}, append(v.str, '\r', '\n')...)
}

// marshallNull marshals a null value into RESP bulk string format.
func (v Value) marshallNull() []byte {
	return []byte("$-1\r\n")
}

// Writer wraps an io.Writer to write RESP formatted data.
type Writer struct {
	writer io.Writer
}

// NewWriter creates a new Writer object.
func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

// Write sends a marshaled Value to the underlying writer.
func (w *Writer) Write(v Value) error {
	bytes := v.Marshal()
	_, err := w.writer.Write(bytes)
	return err
}
