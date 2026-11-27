// Package resp implements a RESP2 reply writer backed by a reusable buffer.
package resp

import (
	"math"
	"strconv"
	"strings"
)

type Writer struct {
	b []byte
}

func NewWriter(size int) *Writer {
	return &Writer{b: make([]byte, 0, size)}
}

// Bytes returns the pending reply data. It aliases the internal buffer.
func (w *Writer) Bytes() []byte { return w.b }

func (w *Writer) Len() int { return len(w.b) }

// Reset clears the buffer but keeps its capacity.
func (w *Writer) Reset() { w.b = w.b[:0] }

// Truncate discards everything written after the first n bytes.
func (w *Writer) Truncate(n int) { w.b = w.b[:n] }

// Shrink releases the buffer if it grew beyond max, to avoid pinning memory
// after a single huge reply.
func (w *Writer) Shrink(max int) {
	if cap(w.b) > max && len(w.b) == 0 {
		w.b = make([]byte, 0, max)
	}
}

func (w *Writer) crlf() { w.b = append(w.b, '\r', '\n') }

// stripNewlines makes s safe to send as a simple string or error.
func stripNewlines(s string) string {
	if strings.ContainsAny(s, "\r\n") {
		s = strings.NewReplacer("\r", " ", "\n", " ").Replace(s)
	}
	return s
}

// WriteString writes a simple string: +s\r\n
func (w *Writer) WriteString(s string) {
	w.b = append(w.b, '+')
	w.b = append(w.b, stripNewlines(s)...)
	w.crlf()
}

// WriteError writes an error: -s\r\n
func (w *Writer) WriteError(s string) {
	w.b = append(w.b, '-')
	w.b = append(w.b, stripNewlines(s)...)
	w.crlf()
}

func (w *Writer) WriteInt(n int) { w.WriteInt64(int64(n)) }

func (w *Writer) WriteInt64(n int64) {
	w.b = append(w.b, ':')
	w.b = strconv.AppendInt(w.b, n, 10)
	w.crlf()
}

func (w *Writer) WriteBulk(b []byte) {
	w.b = append(w.b, '$')
	w.b = strconv.AppendInt(w.b, int64(len(b)), 10)
	w.crlf()
	w.b = append(w.b, b...)
	w.crlf()
}

func (w *Writer) WriteBulkString(s string) {
	w.b = append(w.b, '$')
	w.b = strconv.AppendInt(w.b, int64(len(s)), 10)
	w.crlf()
	w.b = append(w.b, s...)
	w.crlf()
}

// WriteNull writes a null bulk string: $-1\r\n
func (w *Writer) WriteNull() {
	w.b = append(w.b, "$-1\r\n"...)
}

// WriteArray writes an array header; the caller must write exactly n elements.
func (w *Writer) WriteArray(n int) {
	w.b = append(w.b, '*')
	w.b = strconv.AppendInt(w.b, int64(n), 10)
	w.crlf()
}

// WriteFloat writes a float as a bulk string, formatted like redis.
func (w *Writer) WriteFloat(f float64) {
	w.WriteBulk(AppendFloat(nil, f))
}

// AppendFloat formats f with the shortest representation, like redis 7.
func AppendFloat(dst []byte, f float64) []byte {
	switch {
	case math.IsInf(f, 1):
		return append(dst, "inf"...)
	case math.IsInf(f, -1):
		return append(dst, "-inf"...)
	}
	// plain notation for "normal" magnitudes (1000000, not 1e+06)
	if abs := math.Abs(f); abs == 0 || (abs >= 1e-4 && abs < 1e17) {
		return strconv.AppendFloat(dst, f, 'f', -1, 64)
	}
	return strconv.AppendFloat(dst, f, 'g', -1, 64)
}

// WriteBulkStrings writes an array of bulk strings.
func (w *Writer) WriteBulkStrings(ss []string) {
	w.WriteArray(len(ss))
	for _, s := range ss {
		w.WriteBulkString(s)
	}
}

// IsError reports whether b starts with an error reply.
func IsError(b []byte) bool { return len(b) > 0 && b[0] == '-' }
