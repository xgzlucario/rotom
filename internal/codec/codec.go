// Package codec is the binary encoding used by RDB files: little-endian
// fixed-size integers, varints, and uvarint length-prefixed byte strings.
package codec

import (
	"encoding/binary"
	"errors"
	"math"
)

// ErrCorrupted is raised (via panic) by Reader on malformed input. Decoding
// code stays linear this way; LoadRDB recovers it into an error.
var ErrCorrupted = errors.New("corrupted data")

// Encoder is implemented by every value type that can be saved in RDB files.
type Encoder interface {
	Encode(w *Writer)
	Decode(r *Reader)
}

var order = binary.LittleEndian

type Writer struct{ b []byte }

func NewWriter(b []byte) *Writer { return &Writer{b: b} }

func (w *Writer) Bytes() []byte { return w.b }

func (w *Writer) WriteUint8(v uint8)    { w.b = append(w.b, v) }
func (w *Writer) WriteUint32(v uint32)  { w.b = order.AppendUint32(w.b, v) }
func (w *Writer) WriteUint64(v uint64)  { w.b = order.AppendUint64(w.b, v) }
func (w *Writer) WriteVarint(v int64)   { w.b = binary.AppendVarint(w.b, v) }
func (w *Writer) WriteUvarint(v uint64) { w.b = binary.AppendUvarint(w.b, v) }
func (w *Writer) WriteFloat64(v float64) {
	w.WriteUint64(math.Float64bits(v))
}

func (w *Writer) WriteBytes(b []byte) {
	w.WriteUvarint(uint64(len(b)))
	w.b = append(w.b, b...)
}

func (w *Writer) WriteString(s string) {
	w.WriteUvarint(uint64(len(s)))
	w.b = append(w.b, s...)
}

type Reader struct{ b []byte }

func NewReader(b []byte) *Reader { return &Reader{b: b} }

// Len returns the number of unread bytes.
func (r *Reader) Len() int { return len(r.b) }

// next consumes n bytes.
func (r *Reader) next(n int) []byte {
	if n < 0 || n > len(r.b) {
		panic(ErrCorrupted)
	}
	b := r.b[:n:n]
	r.b = r.b[n:]
	return b
}

func (r *Reader) ReadUint8() uint8     { return r.next(1)[0] }
func (r *Reader) ReadUint32() uint32   { return order.Uint32(r.next(4)) }
func (r *Reader) ReadUint64() uint64   { return order.Uint64(r.next(8)) }
func (r *Reader) ReadFloat64() float64 { return math.Float64frombits(r.ReadUint64()) }

func (r *Reader) ReadVarint() int64 {
	v, n := binary.Varint(r.b)
	if n <= 0 {
		panic(ErrCorrupted)
	}
	r.b = r.b[n:]
	return v
}

func (r *Reader) ReadUvarint() uint64 {
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		panic(ErrCorrupted)
	}
	r.b = r.b[n:]
	return v
}

// ReadLen reads a count and checks it is plausible: every element takes at
// least one byte, so a count larger than the remaining input is corrupt.
// This stops a corrupted count from triggering a huge allocation.
func (r *Reader) ReadLen() int {
	n := r.ReadUvarint()
	if n > uint64(len(r.b)) {
		panic(ErrCorrupted)
	}
	return int(n)
}

// ReadBytes returns a copy of the next length-prefixed byte string. The copy
// matters: values are kept and mutated after loading.
func (r *Reader) ReadBytes() []byte {
	b := r.next(r.ReadLen())
	return append(make([]byte, 0, len(b)), b...)
}

func (r *Reader) ReadString() string {
	return string(r.next(r.ReadLen()))
}
