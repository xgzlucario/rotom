package zset

import (
	"cmp"

	"github.com/chen3feng/stl4go"
	"github.com/cockroachdb/swiss"
)

type ZSetI interface {
	Set(key string, score float64) bool
	Get(key string) (float64, bool)
	Remove(key string) bool
	Len() int
	PopMin() (key string, score float64)
	Rank(key string) (rank int, score float64)
	Range(start, stop int, fn func(key string, score float64))
}

var _ ZSetI = (*ZSet)(nil)

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

func NewZSet() *ZSet {
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

func (z *ZSet) Rank(key string) (int, float64) {
	score, ok := z.m.Get(key)
	if !ok {
		return -1, 0
	}
	index := -1
	z.skl.ForEachIf(func(n node, _ struct{}) bool {
		index++
		return n.key != key
	})
	return index, score
}

func (z *ZSet) Range(start, stop int, fn func(key string, score float64)) {
	var index int
	z.skl.ForEachIf(func(n node, _ struct{}) bool {
		if index >= start && index < stop {
			fn(n.key, n.score)
		}
		index++
		return true
	})
}

func (z *ZSet) Len() int {
	return z.m.Len()
}
