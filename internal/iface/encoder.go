package iface

import (
	"encoding/binary"
)

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

func (w *Writer) WriteUint32(n uint32) {
	w.b = order.AppendUint32(w.b, n)
}

func (w *Writer) WriteUint64(n uint64) {
	w.b = order.AppendUint64(w.b, n)
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
	return string(r.ReadBytes())
}

func (r *Reader) ReadBytes() []byte {
	klen, n := binary.Uvarint(r.b)
	key := r.b[n : int(klen)+n]
	r.b = r.b[int(klen)+n:]
	return key
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

func (r *Reader) IsEnd() bool {
	return len(r.b) == 0
}
