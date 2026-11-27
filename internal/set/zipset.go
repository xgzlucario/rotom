package set

import (
	"iter"

	"github.com/xgzlucario/rotom/internal/codec"
	"github.com/xgzlucario/rotom/internal/listpack"
)

// ZipSet stores members unordered in a listpack. Lookups are O(n), which is
// faster than hashing for the small sizes it is used for.
type ZipSet struct {
	lp listpack.ListPack
}

func NewZipSet() *ZipSet { return &ZipSet{} }

// find returns a cursor before member.
func (zs *ZipSet) find(member string) (listpack.Cursor, bool) {
	c := zs.lp.Front()
	for v, ok := c.Next(); ok; v, ok = c.Next() {
		if string(v) == member { // no allocation: compiler optimizes the conversion
			c.Prev()
			return c, true
		}
	}
	return c, false
}

func (zs *ZipSet) Add(member string) bool {
	if zs.Contains(member) {
		return false
	}
	zs.lp.PushBack(member)
	return true
}

func (zs *ZipSet) Contains(member string) bool {
	_, ok := zs.find(member)
	return ok
}

func (zs *ZipSet) Remove(member string) bool {
	c, ok := zs.find(member)
	if ok {
		c.Remove()
	}
	return ok
}

func (zs *ZipSet) Pop() (string, bool) { return zs.lp.PopBack() }

func (zs *ZipSet) Len() int { return zs.lp.Len() }

// All iterates the members. They are only valid until the set is modified.
func (zs *ZipSet) All() iter.Seq[string] {
	return func(yield func(string) bool) {
		for v := range zs.lp.All() {
			if !yield(string(v)) {
				return
			}
		}
	}
}

// ToSet converts to the hash set encoding.
func (zs *ZipSet) ToSet() Set {
	s := make(Set, zs.Len())
	for v := range zs.lp.All() {
		s[string(v)] = struct{}{}
	}
	return s
}

func (zs *ZipSet) Encode(w *codec.Writer) { zs.lp.Encode(w) }
func (zs *ZipSet) Decode(r *codec.Reader) { zs.lp.Decode(r) }
