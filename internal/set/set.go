// Package set implements the redis set type in two encodings: ZipSet, a
// compact listpack for small sets, and Set, a hash set for large ones.
package set

import (
	"iter"
	"maps"

	"github.com/xgzlucario/rotom/internal/codec"
)

// Set is a hash set.
type Set map[string]struct{}

func New() Set { return make(Set) }

func (s Set) Add(member string) bool {
	if _, ok := s[member]; ok {
		return false
	}
	s[member] = struct{}{}
	return true
}

func (s Set) Contains(member string) bool {
	_, ok := s[member]
	return ok
}

func (s Set) Remove(member string) bool {
	if _, ok := s[member]; !ok {
		return false
	}
	delete(s, member)
	return true
}

// Pop removes an arbitrary member: map iteration starts at a random position.
func (s Set) Pop() (string, bool) {
	for member := range s {
		delete(s, member)
		return member, true
	}
	return "", false
}

func (s Set) Len() int { return len(s) }

func (s Set) All() iter.Seq[string] { return maps.Keys(s) }

// Encode writes [count, member...].
func (s Set) Encode(w *codec.Writer) {
	w.WriteUvarint(uint64(len(s)))
	for member := range s {
		w.WriteString(member)
	}
}

func (s Set) Decode(r *codec.Reader) {
	for range r.ReadLen() {
		s[r.ReadString()] = struct{}{}
	}
}
