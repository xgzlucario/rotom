package main

import (
	"bytes"
	"io"
	"strconv"
	"time"
	"unsafe"
)

const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
	MAP     = '%' // TODO: https://redis.io/docs/latest/develop/reference/protocol-spec/#maps
)

var CRLF = []byte("\r\n")

// RESPReader is a reader for RESP (Redis Serialization Protocol) messages.
type RESPReader struct {
	b []byte
}

// NewReader creates a new Resp object with a buffered reader.
func NewReader(input []byte) *RESPReader {
	return &RESPReader{b: input}
}

// parseInt parse first integer from buf.
// input "3\r\nHELLO" -> (3, "HELLO", nil).
func parseInt(buf []byte) (n int, after []byte, err error) {
	for i, b := range buf {
		if b >= '0' && b <= '9' {
			n = n*10 + int(b-'0')
			continue
		}
		if b == '\r' {
			if len(buf) > i+1 && buf[i+1] == '\n' {
				return n, buf[i+2:], nil
			}
			break
		}
		return 0, nil, errParseInteger
	}
	return 0, nil, errCRLFNotFound
}

// ReadNextCommand reads the next RESP command from the RESPReader.
// It parses both `COMMAND_BULK` and `COMMAND_INLINE` formats.
func (r *RESPReader) ReadNextCommand(argsBuf []RESP) (args []RESP, n int, err error) {
	srclen := len(r.b)
	if srclen == 0 {
		return nil, 0, io.EOF
	}
	args = argsBuf[:0]

	switch r.b[0] {
	case ARRAY:
		// command_bulk format
		num, after, err := parseInt(r.b[1:])
		if err != nil {
			return nil, 0, err
		}
		r.b = after

		// read bulk strings for range
		for i := 0; i < num; i++ {
			if len(r.b) == 0 || r.b[0] != BULK {
				return nil, 0, errWrongArguments
			}

			num, after, err := parseInt(r.b[1:])
			if err != nil {
				return nil, 0, err
			}

			// bound check
			if num < 0 || num+2 > len(after) {
				return nil, 0, errWrongArguments
			}

			args = append(args, after[:num])

			// skip CRLF
			r.b = after[num+2:]
		}

	default:
		// command_inline format
		before, after, ok := bytes.Cut(r.b, CRLF)
		if !ok {
			return nil, 0, errWrongArguments
		}
		args = append(args, before)
		r.b = after
	}

	n = srclen - len(r.b)
	return
}

// RESPWriter is a writer that helps construct RESP (Redis Serialization Protocol) messages.
type RESPWriter struct {
	b []byte
}

// NewWriter initializes a new RESPWriter with a given capacity.
func NewWriter(cap int) *RESPWriter {
	return &RESPWriter{make([]byte, 0, cap)}
}

// WriteArrayHead writes the RESP array header with the given length.
func (w *RESPWriter) WriteArrayHead(arrayLen int) {
	w.b = append(w.b, ARRAY)
	w.b = strconv.AppendUint(w.b, uint64(arrayLen), 10)
	w.b = append(w.b, CRLF...)
}

// WriteBulk writes a RESP bulk string from a byte slice.
func (w *RESPWriter) WriteBulk(bulk []byte) {
	w.WriteBulkString(b2s(bulk))
}

// WriteBulkString writes a RESP bulk string from a string.
func (w *RESPWriter) WriteBulkString(bulk string) {
	w.b = append(w.b, BULK)
	w.b = strconv.AppendUint(w.b, uint64(len(bulk)), 10)
	w.b = append(w.b, CRLF...)
	w.b = append(w.b, bulk...)
	w.b = append(w.b, CRLF...)
}

// WriteError writes a RESP error message.
func (w *RESPWriter) WriteError(err error) {
	w.b = append(w.b, ERROR)
	w.b = append(w.b, err.Error()...)
	w.b = append(w.b, CRLF...)
}

// WriteString writes a RESP simple string.
func (w *RESPWriter) WriteString(str string) {
	w.b = append(w.b, STRING)
	w.b = append(w.b, str...)
	w.b = append(w.b, CRLF...)
}

// WriteInteger writes a RESP integer.
func (w *RESPWriter) WriteInteger(num int) {
	w.b = append(w.b, INTEGER)
	w.b = strconv.AppendUint(w.b, uint64(num), 10)
	w.b = append(w.b, CRLF...)
}

// WriteFloat writes a RESP bulk string from a float64.
func (w *RESPWriter) WriteFloat(num float64) {
	w.WriteBulkString(strconv.FormatFloat(num, 'f', -1, 64))
}

// WriteNull writes a RESP null bulk string.
func (w *RESPWriter) WriteNull() {
	w.b = append(w.b, "$-1\r\n"...)
}

// Reset resets the internal buffer.
func (w *RESPWriter) Reset() { w.b = w.b[:0] }

// RESP represents the RESP (Redis Serialization Protocol) message in byte slice format.
type RESP []byte

func (r RESP) ToString() string { return string(r) }

func (r RESP) ToStringUnsafe() string { return b2s(r) }

func (r RESP) ToInt() (int, error) { return strconv.Atoi(b2s(r)) }

func (r RESP) ToDuration() (time.Duration, error) {
	n, err := strconv.Atoi(b2s(r))
	return time.Duration(n), err
}

func (r RESP) ToFloat() (float64, error) { return strconv.ParseFloat(b2s(r), 64) }

func (r RESP) Clone() []byte { return bytes.Clone(r) }

func b2s(b []byte) string { return *(*string)(unsafe.Pointer(&b)) }
