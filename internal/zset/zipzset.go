package zset

import (
	"encoding/binary"
	"iter"
	"math"
	"strings"
	"unsafe"

	"github.com/xgzlucario/rotom/internal/codec"
	"github.com/xgzlucario/rotom/internal/listpack"
)

// ZipZSet keeps entries sorted by (score, member) in a listpack. Every
// operation is O(n), which is fast for the small sizes it is used for.
//
// entry: | score (8 bytes, little endian float64) | member |
type ZipZSet struct {
	lp listpack.ListPack
}

func NewZipZSet() *ZipZSet { return &ZipZSet{} }

func encodeEntry(member string, score float64) string {
	b := make([]byte, 8, 8+len(member))
	binary.LittleEndian.PutUint64(b, math.Float64bits(score))
	b = append(b, member...)
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// decodeEntry returns a member aliasing entry.
func decodeEntry(entry []byte) (string, float64) {
	score := math.Float64frombits(binary.LittleEndian.Uint64(entry))
	return unsafe.String(unsafe.SliceData(entry[8:]), len(entry)-8), score
}

func (zs *ZipZSet) Len() int { return zs.lp.Len() }

// find returns a cursor before member, its rank and score.
func (zs *ZipZSet) find(member string) (c listpack.Cursor, rank int, score float64) {
	c = zs.lp.Front()
	for v, ok := c.Next(); ok; v, ok = c.Next() {
		m, s := decodeEntry(v)
		if m == member {
			c.Prev()
			return c, rank, s
		}
		rank++
	}
	return c, -1, 0
}

func (zs *ZipZSet) Get(member string) (float64, bool) {
	_, rank, score := zs.find(member)
	return score, rank >= 0
}

func (zs *ZipZSet) Set(member string, score float64) bool {
	c, rank, old := zs.find(member)
	if rank >= 0 {
		if old == score {
			return false
		}
		c.Remove()
	}
	// insert before the first entry that sorts after (score, member)
	c = zs.lp.Front()
	for v, ok := c.Next(); ok; v, ok = c.Next() {
		m, s := decodeEntry(v)
		if s > score || (s == score && m > member) {
			c.Prev()
			break
		}
	}
	c.Insert(encodeEntry(member, score))
	return rank < 0
}

func (zs *ZipZSet) Remove(member string) bool {
	c, rank, _ := zs.find(member)
	if rank >= 0 {
		c.Remove()
	}
	return rank >= 0
}

// PopMin removes the first entry. The set must not be empty.
func (zs *ZipZSet) PopMin() (string, float64) {
	entry, _ := zs.lp.PopFront()
	return decodeEntry([]byte(entry))
}

func (zs *ZipZSet) Rank(member string) int {
	_, rank, _ := zs.find(member)
	return rank
}

func (zs *ZipZSet) All() iter.Seq2[string, float64] { return zs.Range(0, zs.Len()-1) }

// Range iterates ranks start..stop inclusive. Members alias the listpack
// and are only valid until it is modified.
func (zs *ZipZSet) Range(start, stop int) iter.Seq2[string, float64] {
	return func(yield func(string, float64) bool) {
		i := 0
		for v := range zs.lp.All() {
			if i > stop {
				return
			}
			if i >= start {
				if !yield(decodeEntry(v)) {
					return
				}
			}
			i++
		}
	}
}

// ToZSet converts to the skiplist encoding.
func (zs *ZipZSet) ToZSet() *ZSet {
	z := New()
	for member, score := range zs.All() {
		z.Set(strings.Clone(member), score) // member aliases the listpack
	}
	return z
}

func (zs *ZipZSet) Encode(w *codec.Writer) { zs.lp.Encode(w) }

func (zs *ZipZSet) Decode(r *codec.Reader) {
	zs.lp.Decode(r)
	for v := range zs.lp.All() {
		if len(v) < 8 {
			panic(codec.ErrCorrupted)
		}
	}
}
