package structx

import (
	rbtree "github.com/sakeven/RbTree"
)

type ZSet struct {
	m    map[string]int64
	tree *rbtree.Tree[int64, string]
}

func NewZSet() *ZSet {
	return &ZSet{
		m:    map[string]int64{},
		tree: rbtree.NewTree[int64, string](),
	}
}

func (z *ZSet) Get(key string) (int64, bool) {
	s, ok := z.m[key]
	return s, ok
}

func (z *ZSet) Set(key string, score int64) bool {
	z.deleteNode(score, key)
	_, ok := z.m[key]
	z.m[key] = score
	z.tree.Insert(score, key)
	return !ok
}

func (z *ZSet) deleteNode(score int64, key string) bool {
	for it := z.tree.FindIt(score); it != nil; it = it.Next() {
		if it.Value == key {
			z.tree.Delete(it.Key)
			return true
		}
		if it.Key != score {
			return false
		}
	}
	return false
}

func (z *ZSet) Incr(key string, incr int64) int64 {
	score, ok := z.m[key]
	if ok {
		z.deleteNode(score, key)
	}
	score += incr
	z.m[key] = score
	z.tree.Insert(score, key)
	return score
}

func (z *ZSet) Delete(key string) (s int64, ok bool) {
	score, ok := z.m[key]
	if ok {
		delete(z.m, key)
		z.deleteNode(score, key)
	}
	return score, ok
}

func (z *ZSet) Len() int {
	return len(z.m)
}

func (z *ZSet) Iter(f func(k string, s int64) bool) {
	for it := z.tree.Iterator(); it != nil; it = it.Next() {
		if f(it.Value, it.Key) {
			return
		}
	}
}
