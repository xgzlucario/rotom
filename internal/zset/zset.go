// Package zset implements the redis sorted set type in two encodings:
// ZipZSet, a sorted listpack for small sets, and ZSet, a map plus a skiplist
// for large ones. Members are ordered by (score, member).
package zset

import (
	"iter"

	"github.com/xgzlucario/rotom/internal/codec"
)

// ZSet pairs a map, for O(1) member -> score lookups, with a skiplist with
// spans, for O(log n) ordered and rank-based access.
type ZSet struct {
	m   map[string]float64
	skl *skiplist
}

func New() *ZSet {
	return &ZSet{m: make(map[string]float64), skl: newSkiplist()}
}

func (z *ZSet) Len() int { return len(z.m) }

func (z *ZSet) Get(member string) (float64, bool) {
	score, ok := z.m[member]
	return score, ok
}

// Set adds member or updates its score. It reports whether member is new.
func (z *ZSet) Set(member string, score float64) bool {
	old, ok := z.m[member]
	if ok {
		if score != old {
			z.m[member] = score
			z.skl.updateScore(old, member, score)
		}
		return false
	}
	z.m[member] = score
	z.skl.insert(score, member)
	return true
}

func (z *ZSet) Remove(member string) bool {
	score, ok := z.m[member]
	if !ok {
		return false
	}
	delete(z.m, member)
	z.skl.delete(score, member)
	return true
}

// PopMin removes the lowest ranked member. The set must not be empty.
func (z *ZSet) PopMin() (string, float64) {
	x := z.skl.first()
	delete(z.m, x.member)
	z.skl.delete(x.score, x.member)
	return x.member, x.score
}

// Rank returns the 0-based rank of member, or -1. O(log n).
func (z *ZSet) Rank(member string) int {
	score, ok := z.m[member]
	if !ok {
		return -1
	}
	return z.skl.rank(score, member) - 1
}

func (z *ZSet) All() iter.Seq2[string, float64] { return z.Range(0, z.Len()-1) }

// Range iterates ranks start..stop inclusive, which the caller must have
// clamped to [0, Len()). O(log n + stop - start).
func (z *ZSet) Range(start, stop int) iter.Seq2[string, float64] {
	return func(yield func(string, float64) bool) {
		x := z.skl.byRank(start + 1)
		for i := start; i <= stop && x != nil; i, x = i+1, x.level[0].forward {
			if !yield(x.member, x.score) {
				return
			}
		}
	}
}

// Encode writes [count, member, score, ...] in rank order.
func (z *ZSet) Encode(w *codec.Writer) {
	w.WriteUvarint(uint64(z.Len()))
	for member, score := range z.All() {
		w.WriteString(member)
		w.WriteFloat64(score)
	}
}

func (z *ZSet) Decode(r *codec.Reader) {
	for range r.ReadLen() {
		z.Set(r.ReadString(), r.ReadFloat64())
	}
}
