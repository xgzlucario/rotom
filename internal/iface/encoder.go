package iface

import (
	"bytes"
	"encoding/binary"
	"errors"
)

// ErrCorrupted is raised (via panic) by Reader on malformed input.
var ErrCorrupted = errors.New("corrupted data")

var order = binary.LittleEndian

type Writer struct {
	b []byte
}

func NewWriter(b []byte) *Writer {
	return &Writer{b: b}
}

func (w *Writer) WriteString(key string) {
	w.b = binary.AppendUvarint(w.b, uint64(len(key)))
	w.b = append(w.b, key...)
}

func (w *Writer) WriteBytes(key []byte) {
	w.b = binary.AppendUvarint(w.b, uint64(len(key)))
	w.b = append(w.b, key...)
}

func (w *Writer) WriteUint8(n uint8) {
	w.b = append(w.b, n)
}

func (w *Writer) WriteUint32(n uint32) {
	w.b = order.AppendUint32(w.b, n)
}

func (w *Writer) WriteUint64(n uint64) {
	w.b = order.AppendUint64(w.b, n)
}

func (w *Writer) WriteVarint(n int) {
	w.b = binary.AppendVarint(w.b, int64(n))
}

func (w *Writer) Bytes() []byte { return w.b }

func (w *Writer) Reset() { w.b = w.b[:0] }

type Reader struct {
	b []byte
}

func NewReader(b []byte) *Reader {
	return &Reader{b: b}
}

func NewReaderFrom(w *Writer) *Reader {
	return &Reader{b: w.b}
}

func (r *Reader) ReadString() string {
	return string(r.readBytes())
}

// ReadBytes returns a copy of the next length-prefixed byte slice.
// The copy matters: callers keep and mutate the result (e.g. INCR, listpack
// removal), so it must not alias the reader's underlying buffer.
func (r *Reader) ReadBytes() []byte {
	return bytes.Clone(r.readBytes())
}

func (r *Reader) readBytes() []byte {
	klen, n := binary.Uvarint(r.b)
	if n <= 0 || uint64(len(r.b)-n) < klen {
		panic(ErrCorrupted)
	}
	key := r.b[n : int(klen)+n]
	r.b = r.b[int(klen)+n:]
	return key
}

func (r *Reader) ReadUint8() uint8 {
	n := r.b[0]
	r.b = r.b[1:]
	return n
}

func (r *Reader) ReadUint32() uint32 {
	n := order.Uint32(r.b)
	r.b = r.b[4:]
	return n
}

func (r *Reader) ReadUint64() uint64 {
	n := order.Uint64(r.b)
	r.b = r.b[8:]
	return n
}

func (r *Reader) ReadVarint() int64 {
	x, n := binary.Varint(r.b)
	r.b = r.b[n:]
	return x
}
