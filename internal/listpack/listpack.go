// Package listpack implements a compact list of strings stored in a single
// byte slice, after redis' listpack. It is the building block of the
// compact encodings: QuickList nodes, ZipSet and ZipZSet.
package listpack

import (
	"encoding/binary"
	"iter"
	"math/bits"

	"github.com/klauspost/rvarint"
	"github.com/xgzlucario/rotom/internal/codec"
)

// ListPack layout:
//
//	+--------+--------+-----+--------+
//	| entry0 | entry1 | ... | entryN |
//	+--------+--------+-----+--------+
//
//	entry:
//	+------------+--------------+---------------------+
//	|  data_len  |     data     |       data_len      |
//	+------------+--------------+---------------------+
//	|<- varint ->|<- data_len ->|<- varint(reverse) ->|
//
// The length is stored on both sides, so the list can be walked in both
// directions, and there are no cascading updates as in redis' old ziplist.
type ListPack struct {
	data []byte
	n    int
}

func New() *ListPack { return &ListPack{} }

// Len returns the number of entries.
func (lp *ListPack) Len() int { return lp.n }

// Size returns the encoded size in bytes.
func (lp *ListPack) Size() int { return len(lp.data) }

func (lp *ListPack) PushFront(v string) { c := lp.Front(); c.Insert(v) }
func (lp *ListPack) PushBack(v string)  { c := lp.Back(); c.Insert(v) }

// PopFront removes and returns the first entry.
func (lp *ListPack) PopFront() (string, bool) {
	if lp.n == 0 {
		return "", false
	}
	c := lp.Front()
	return c.Remove(), true
}

// PopBack removes and returns the last entry.
func (lp *ListPack) PopBack() (string, bool) {
	if lp.n == 0 {
		return "", false
	}
	c := lp.Back()
	c.Prev()
	return c.Remove(), true
}

// All iterates entries front to back. The values alias the listpack and
// are only valid until it is modified.
func (lp *ListPack) All() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		c := lp.Front()
		for v, ok := c.Next(); ok; v, ok = c.Next() {
			if !yield(v) {
				return
			}
		}
	}
}

// Backward iterates entries back to front.
func (lp *ListPack) Backward() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		c := lp.Back()
		for v, ok := c.Prev(); ok; v, ok = c.Prev() {
			if !yield(v) {
				return
			}
		}
	}
}

// Cursor is a position between two entries.
type Cursor struct {
	lp  *ListPack
	pos int
}

// Front returns a cursor before the first entry.
func (lp *ListPack) Front() Cursor { return Cursor{lp: lp} }

// Back returns a cursor after the last entry.
func (lp *ListPack) Back() Cursor { return Cursor{lp: lp, pos: len(lp.data)} }

// Next returns the entry after the cursor and moves past it.
//
//	  pos      dataStart      dataEnd    next pos
//	   |           |              |          |
//	   +-----------+--------------+----------+
//	-> |  data_len |     data     | data_len |
//	   +-----------+--------------+----------+
func (c *Cursor) Next() ([]byte, bool) {
	if c.pos == len(c.lp.data) {
		return nil, false
	}
	size, n := binary.Uvarint(c.lp.data[c.pos:])
	start := c.pos + n
	end := start + int(size)
	c.pos = end + n
	return c.lp.data[start:end:end], true
}

// Prev returns the entry before the cursor and moves before it.
func (c *Cursor) Prev() ([]byte, bool) {
	if c.pos == 0 {
		return nil, false
	}
	size, n := rvarint.Uvarint(c.lp.data[:c.pos])
	end := c.pos - n
	start := end - int(size)
	c.pos = start - n
	return c.lp.data[start:end:end], true
}

// Insert inserts v at the cursor and moves past it.
func (c *Cursor) Insert(v string) {
	lp := c.lp
	if c.pos == len(lp.data) {
		lp.data = appendEntry(lp.data, v)
	} else {
		size := entrySize(len(v))
		lp.data = append(lp.data, make([]byte, size)...) // grow
		copy(lp.data[c.pos+size:], lp.data[c.pos:])
		appendEntry(lp.data[c.pos:c.pos], v) // writes into the gap, in place
	}
	c.pos += entrySize(len(v))
	lp.n++
}

// Remove deletes the entry after the cursor and returns a copy of it.
func (c *Cursor) Remove() string {
	start := c.pos
	v, _ := c.Next()
	res := string(v)
	c.lp.data = append(c.lp.data[:start], c.lp.data[c.pos:]...)
	c.pos = start
	c.lp.n--
	return res
}

func entrySize(n int) int { return n + 2*uvarintSize(uint64(n)) }

func appendEntry(dst []byte, v string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(v)))
	dst = append(dst, v...)
	return rvarint.AppendUvarint(dst, uint64(len(v)))
}

// uvarintSize returns the encoded size of x.
func uvarintSize(x uint64) int { return (9*bits.Len64(x) + 64) / 64 }

// Encode writes [count, data].
func (lp *ListPack) Encode(w *codec.Writer) {
	w.WriteUvarint(uint64(lp.n))
	w.WriteBytes(lp.data)
}

// Decode reads what Encode wrote and validates the entries.
func (lp *ListPack) Decode(r *codec.Reader) {
	n := r.ReadUvarint()
	lp.data = r.ReadBytes()
	lp.n = 0
	for pos := 0; pos < len(lp.data); lp.n++ {
		size, k := binary.Uvarint(lp.data[pos:])
		end := pos + k + int(size) + k
		if k <= 0 || size > uint64(len(lp.data)) || end > len(lp.data) {
			panic(codec.ErrCorrupted)
		}
		pos = end
	}
	if uint64(lp.n) != n {
		panic(codec.ErrCorrupted)
	}
}
