package structx

import (
	"sync"

	rbtree "github.com/sakeven/RbTree"
	"golang.org/x/exp/constraints"
)

type Ordered constraints.Ordered

// ZSet
type ZSet[K, S Ordered] struct {
	sync.RWMutex
	m    Map[K, S]
	tree *rbtree.Tree[S, K]
}

// NewZSet
func NewZSet[K, S Ordered]() *ZSet[K, S] {
	return &ZSet[K, S]{
		m:    NewMap[K, S](),
		tree: rbtree.NewTree[S, K](),
	}
}

// Get
func (z *ZSet[K, S]) Get(key K) (S, bool) {
	z.RLock()
	defer z.RUnlock()
	return z.m.Get(key)
}

// Set upsert value by key.
func (z *ZSet[K, S]) Set(key K, score S) {
	z.Lock()
	z.set(key, score)
	z.Unlock()
}

func (z *ZSet[K, S]) set(key K, score S) {
	z.deleteNode(score, key)
	z.m.Put(key, score)
	z.tree.Insert(score, key)
}

// deleteNode
func (z *ZSet[K, S]) deleteNode(score S, key K) bool {
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

// Incr
func (z *ZSet[K, S]) Incr(key K, incr S) S {
	z.Lock()
	score, ok := z.m.Get(key)
	if ok {
		z.deleteNode(score, key)
	}
	score += incr
	z.m.Put(key, score)
	z.tree.Insert(score, key)
	z.Unlock()
	return score
}

// Delete
func (z *ZSet[K, S]) Delete(key K) (s S, ok bool) {
	z.Lock()
	score, ok := z.m.Get(key)
	if ok {
		z.m.Delete(key)
		z.deleteNode(score, key)
	}
	z.Unlock()
	return score, ok
}

// Len
func (z *ZSet[K, S]) Len() int {
	z.RLock()
	defer z.RUnlock()
	return z.m.Count()
}

// Iter iterate all elements by scores.
func (z *ZSet[K, S]) Iter(f func(k K, s S) bool) {
	z.RLock()
	defer z.RUnlock()

	for it := z.tree.Iterator(); it != nil; it = it.Next() {
		if f(it.Value, it.Key) {
			return
		}
	}
}

// MarshalJSON
func (z *ZSet[K, S]) MarshalJSON() ([]byte, error) {
	return z.m.MarshalJSON()
}

// UnmarshalJSON
func (z *ZSet[K, S]) UnmarshalJSON(src []byte) error {
	if err := z.m.UnmarshalJSON(src); err != nil {
		return err
	}

	z.m.Iter(func(k K, s S) bool {
		z.tree.Insert(s, k)
		return false
	})

	return nil
}
