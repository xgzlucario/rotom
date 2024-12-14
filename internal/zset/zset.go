package zset

import (
	"cmp"
	"github.com/chen3feng/stl4go"
	"github.com/cockroachdb/swiss"
	"github.com/xgzlucario/rotom/internal/iface"
	"math"
)

var (
	_ iface.ZSetI = (*ZSet)(nil)
)

type node struct {
	key   string
	score float64
}

func nodeCompare(a, b node) int {
	if a.score == b.score {
		return cmp.Compare(a.key, b.key)
	}
	return cmp.Compare(a.score, b.score)
}

type ZSet struct {
	m   *swiss.Map[string, float64]
	skl *stl4go.SkipList[node, struct{}]
}

func New() *ZSet {
	return &ZSet{
		m:   swiss.New[string, float64](8),
		skl: stl4go.NewSkipListFunc[node, struct{}](nodeCompare),
	}
}

func (z *ZSet) Get(key string) (float64, bool) {
	return z.m.Get(key)
}

func (z *ZSet) Set(key string, score float64) bool {
	old, ok := z.m.Get(key)
	if ok {
		// same
		if score == old {
			return false
		}
		z.skl.Remove(node{key, old})
	}
	z.m.Put(key, score)
	z.skl.Insert(node{key, score}, struct{}{})
	return !ok
}

func (z *ZSet) Remove(key string) bool {
	score, ok := z.m.Get(key)
	if !ok {
		return false
	}
	z.m.Delete(key)
	z.skl.Remove(node{key, score})
	return true
}

func (z *ZSet) PopMin() (key string, score float64) {
	z.skl.ForEachIf(func(n node, _ struct{}) bool {
		key = n.key
		score = n.score
		return false
	})
	z.m.Delete(key)
	z.skl.Remove(node{key, score})
	return
}

func (z *ZSet) Rank(key string) (index int) {
	index = -1
	_, ok := z.m.Get(key)
	if !ok {
		return
	}
	z.skl.ForEachIf(func(n node, _ struct{}) bool {
		index++
		return n.key != key
	})
	return
}

func (z *ZSet) Scan(fn func(key string, score float64)) {
	z.skl.ForEachIf(func(n node, _ struct{}) bool {
		fn(n.key, n.score)
		return true
	})
}

func (z *ZSet) Len() int {
	return z.m.Len()
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
	w.WriteUint64(uint64(z.m.Len()))
	z.Scan(func(key string, score float64) {
		w.WriteString(key)
		w.WriteUint64(math.Float64bits(score))
	})
}
