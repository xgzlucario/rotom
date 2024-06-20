package main

import (
	"bytes"
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
)

var (
	CRLF = []byte("\r\n")
)

// RESPReader is a reader for RESP (Redis Serialization Protocol) messages.
type RESPReader struct {
	b []byte
}

// NewReader creates a new Resp object with a buffered reader.
func NewReader(input []byte) *RESPReader {
	return &RESPReader{b: input}
}

// cutByCRLF splits the buffer by the first occurrence of CRLF.
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

// ReadNextCommand reads the next RESP command from the RESPReader.
// It parses both COMMAND_BULK and COMMAND_INLINE formats.
func (r *RESPReader) ReadNextCommand(argsBuf []RESP) (args []RESP, err error) {
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

// RESPWriter is a writer that helps construct RESP (Redis Serialization Protocol) messages.
type RESPWriter struct {
	b *bytes.Buffer
}

// NewWriter initializes a new RESPWriter with a given capacity.
func NewWriter(cap int) *RESPWriter {
	return &RESPWriter{bytes.NewBuffer(make([]byte, 0, cap))}
}

// WriteArrayHead writes the RESP array header with the given length.
func (w *RESPWriter) WriteArrayHead(arrayLen int) {
	w.b.WriteByte(ARRAY)
	w.b.WriteString(strconv.Itoa(arrayLen))
	w.b.Write(CRLF)
}

// WriteBulk writes a RESP bulk string from a byte slice.
func (w *RESPWriter) WriteBulk(bluk []byte) {
	w.WriteBulkString(b2s(bluk))
}

// WriteBulkString writes a RESP bulk string from a string.
func (w *RESPWriter) WriteBulkString(bluk string) {
	w.b.WriteByte(BULK)
	w.b.WriteString(strconv.Itoa(len(bluk)))
	w.b.Write(CRLF)
	w.b.WriteString(bluk)
	w.b.Write(CRLF)
}

// WriteError writes a RESP error message.
func (w *RESPWriter) WriteError(err error) {
	w.b.WriteByte(ERROR)
	w.b.WriteString(err.Error())
	w.b.Write(CRLF)
}

// WriteString writes a RESP simple string.
func (w *RESPWriter) WriteString(str string) {
	w.b.WriteByte(STRING)
	w.b.WriteString(str)
	w.b.Write(CRLF)
}

// WriteInteger writes a RESP integer.
func (w *RESPWriter) WriteInteger(num int) {
	w.b.WriteByte(INTEGER)
	w.b.WriteString(strconv.Itoa(num))
	w.b.Write(CRLF)
}

// WriteNull writes a RESP null bulk string.
func (w *RESPWriter) WriteNull() {
	w.b.WriteString("$-1")
	w.b.Write(CRLF)
}

// Reset resets the internal buffer.
func (w *RESPWriter) Reset() {
	w.b.Reset()
}

// RESP represents the RESP (Redis Serialization Protocol) message in byte slice format.
type RESP []byte

func (r RESP) ToString() string {
	return string(r)
}

func (r RESP) ToStringUnsafe() string {
	return b2s(r)
}

func (r RESP) ToInt() (int, error) {
	return strconv.Atoi(b2s(r))
}

func (r RESP) ToBytes() []byte {
	return r
}

func (r RESP) Clone() []byte {
	return slices.Clone(r)
}

func b2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
