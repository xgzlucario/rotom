// Package hash implements the redis hash type.
package hash

import (
	"encoding/binary"
	"iter"
	"strings"

	"github.com/xgzlucario/rotom/internal/codec"
)

// ZipMap stores all values back to back in one byte slice, and maps each
// field to the offset of its value:
//
//	index: {"f1": 0, "f2": 6}
//	data:  | len | value1 | len | value2 | ...
//
// Compared to map[string][]byte this saves one allocation (and one object
// for the GC to scan) per value. Overwriting a value with one of a different
// length leaves garbage behind, which is compacted once it gets large.
type ZipMap struct {
	index  map[string]uint32
	data   []byte
	unused int // garbage bytes in data
}

const (
	compactMinUnused = 1024 // bytes
	compactMinRatio  = 0.5  // garbage / total
)

func New() *ZipMap {
	return &ZipMap{index: make(map[string]uint32, 8)}
}

func (zm *ZipMap) Len() int { return len(zm.index) }

func (zm *ZipMap) Get(field string) ([]byte, bool) {
	pos, ok := zm.index[field]
	if !ok {
		return nil, false
	}
	val, _ := zm.value(pos)
	return val, true
}

// Set sets field to a copy of val and reports whether field is new.
// field is copied too, so it may alias a reused buffer.
func (zm *ZipMap) Set(field string, val []byte) bool {
	pos, ok := zm.index[field]
	if ok {
		old, size := zm.value(pos)
		if len(old) == len(val) {
			copy(old, val) // update in place
			return false
		}
		zm.unused += size
	}
	// Always store an owned key: assigning to an existing map key also
	// replaces the stored key with the one passed in.
	zm.index[strings.Clone(field)] = uint32(len(zm.data))
	zm.data = appendValue(zm.data, val)
	zm.compactIfNeeded()
	return !ok
}

func (zm *ZipMap) Remove(field string) bool {
	pos, ok := zm.index[field]
	if !ok {
		return false
	}
	_, size := zm.value(pos)
	zm.unused += size
	delete(zm.index, field)
	zm.compactIfNeeded()
	return true
}

// All iterates fields and values. Values alias the map and are only valid
// until it is modified.
func (zm *ZipMap) All() iter.Seq2[string, []byte] {
	return func(yield func(string, []byte) bool) {
		for field, pos := range zm.index {
			val, _ := zm.value(pos)
			if !yield(field, val) {
				return
			}
		}
	}
}

// value returns the value at pos and its encoded size.
func (zm *ZipMap) value(pos uint32) ([]byte, int) {
	size, n := binary.Uvarint(zm.data[pos:])
	start := int(pos) + n
	end := start + int(size)
	return zm.data[start:end:end], n + int(size)
}

func appendValue(dst, val []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(val)))
	return append(dst, val...)
}

// compactIfNeeded rewrites data without garbage once garbage is both large
// in absolute terms and the majority of data.
func (zm *ZipMap) compactIfNeeded() {
	if zm.unused < compactMinUnused || float64(zm.unused) < compactMinRatio*float64(len(zm.data)) {
		return
	}
	data := make([]byte, 0, len(zm.data)-zm.unused)
	for field, pos := range zm.index {
		val, _ := zm.value(pos)
		zm.index[field] = uint32(len(data))
		data = appendValue(data, val)
	}
	zm.data = data
	zm.unused = 0
}

// Encode writes [count, field, value, ...].
func (zm *ZipMap) Encode(w *codec.Writer) {
	w.WriteUvarint(uint64(zm.Len()))
	for field, val := range zm.All() {
		w.WriteString(field)
		w.WriteBytes(val)
	}
}

func (zm *ZipMap) Decode(r *codec.Reader) {
	for range r.ReadLen() {
		zm.Set(r.ReadString(), r.ReadBytes())
	}
}
