package zset

import (
	"math"

	"github.com/xgzlucario/rotom/internal/iface"
)

var (
	_ iface.ZSetI = (*ZSet)(nil)
)

// ZSet is the redis sorted set: a map for member -> score lookups plus a
// skiplist with spans for ordered and rank-based access.
type ZSet struct {
	m   map[string]float64
	skl *skiplist
}

func New() *ZSet {
	return &ZSet{
		m:   make(map[string]float64, 8),
		skl: newSkiplist(),
	}
}

func (z *ZSet) Get(key string) (float64, bool) {
	score, ok := z.m[key]
	return score, ok
}

func (z *ZSet) Set(key string, score float64) bool {
	old, ok := z.m[key]
	if ok {
		if score == old {
			return false
		}
		z.skl.delete(old, key)
	}
	z.m[key] = score
	z.skl.insert(score, key)
	return !ok
}

func (z *ZSet) Remove(key string) bool {
	score, ok := z.m[key]
	if !ok {
		return false
	}
	delete(z.m, key)
	z.skl.delete(score, key)
	return true
}

func (z *ZSet) PopMin() (key string, score float64) {
	x := z.skl.first()
	if x == nil {
		return "", 0
	}
	key, score = x.member, x.score
	delete(z.m, key)
	z.skl.delete(score, key)
	return
}

// Rank returns the 0-based rank of key, or -1. O(log n).
func (z *ZSet) Rank(key string) int {
	score, ok := z.m[key]
	if !ok {
		return -1
	}
	return z.skl.rank(score, key) - 1
}

func (z *ZSet) Scan(fn func(key string, score float64)) {
	for x := z.skl.first(); x != nil; x = x.level[0].forward {
		fn(x.member, x.score)
	}
}

// Range calls fn for 0-based ranks start..stop (inclusive, already clamped
// by the caller). O(log n + stop - start).
func (z *ZSet) Range(start, stop int, fn func(key string, score float64)) {
	x := z.skl.byRank(start + 1)
	for i := start; i <= stop && x != nil; i++ {
		fn(x.member, x.score)
		x = x.level[0].forward
	}
}

func (z *ZSet) Len() int {
	return len(z.m)
}

func (z *ZSet) ReadFrom(rd *iface.Reader) {
	n := rd.ReadUint64()
	for range n {
		key := rd.ReadString()
		x := rd.ReadUint64()
		score := math.Float64frombits(x)
		z.Set(key, score)
	}
}

func (z *ZSet) WriteTo(w *iface.Writer) {
	w.WriteUint64(uint64(len(z.m)))
	z.Scan(func(key string, score float64) {
		w.WriteString(key)
		w.WriteUint64(math.Float64bits(score))
	})
}
