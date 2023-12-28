package structx

import (
	"encoding/json"

	rbtree "github.com/sakeven/RbTree"
	"golang.org/x/exp/constraints"
)

type Ordered constraints.Ordered

// ZSet
type ZSet[K, S Ordered, V any] struct {
	m    Map[K, *znode[S, V]]
	tree *rbtree.Tree[S, K]
}

type znode[S Ordered, V any] struct {
	S S
	V V
}

// NewZSet
func NewZSet[K, S Ordered, V any]() *ZSet[K, S, V] {
	return &ZSet[K, S, V]{
		m:    NewMap[K, *znode[S, V]](),
		tree: rbtree.NewTree[S, K](),
	}
}

// Get
func (z *ZSet[K, S, V]) Get(key K) (V, S, bool) {
	item, ok := z.m.Get(key)
	if !ok {
		var v V
		var s S
		return v, s, false
	}
	return item.V, item.S, ok
}

// Set upsert value by key.
func (z *ZSet[K, S, V]) Set(key K, value V) {
	item, ok := z.m.Get(key)
	if ok {
		item.V = value

	} else {
		item = &znode[S, V]{V: value}
		z.m.Put(key, item)
		z.tree.Insert(item.S, key)
	}
}

// SetScore upsert score by key.
func (z *ZSet[K, S, V]) SetScore(key K, score S) {
	item, ok := z.m.Get(key)
	if ok {
		z.updateScore(item, key, score)

	} else {
		z.m.Put(key, &znode[S, V]{S: score})
		z.tree.Insert(score, key)
	}
}

// update score of key.
func (z *ZSet[K, S, V]) updateScore(node *znode[S, V], key K, score S) {
	z.tree.Delete(node.S)
	node.S = score
	z.tree.Insert(score, key)
}

// SetWithScore upsert value and score by key.
func (z *ZSet[K, S, V]) SetWithScore(key K, score S, value V) {
	item, ok := z.m.Get(key)
	if ok {
		item.V = value
		z.updateScore(item, key, score)

	} else {
		z.m.Put(key, &znode[S, V]{S: score, V: value})
		z.tree.Insert(score, key)
	}
}

// Incr
func (z *ZSet[K, S, V]) Incr(key K, score S) S {
	item, ok := z.m.Get(key)
	if ok {
		z.updateScore(item, key, item.S+score)
		return item.S

	} else {
		z.m.Put(key, &znode[S, V]{S: score})
		z.tree.Insert(score, key)
		return score
	}
}

// Delete
func (z *ZSet[K, S, V]) Delete(key K) (v V, ok bool) {
	item, ok := z.m.Get(key)
	if ok {
		z.m.Delete(key)
		z.tree.Delete(item.S)
		return item.V, ok
	}
	return
}

// Len
func (z *ZSet[K, S, V]) Len() int {
	return z.m.Count()
}

// Iter iterate all elements by scores.
func (z *ZSet[K, S, V]) Iter(f func(k K, s S, v V) bool) {
	iter := z.tree.Iterator()
	for iter != nil {
		item, _ := z.m.Get(iter.Value)
		if f(iter.Value, iter.Key, item.V) {
			return
		}
		iter = iter.Next()
	}
}

type zsetJSON[K, S Ordered, V any] struct {
	K []K
	S []S
	V []V
}

// MarshalJSON
func (z *ZSet[K, S, V]) MarshalJSON() ([]byte, error) {
	tmp := zsetJSON[K, S, V]{
		K: make([]K, 0, z.Len()),
		S: make([]S, 0, z.Len()),
		V: make([]V, 0, z.Len()),
	}
	z.m.Iter(func(k K, item *znode[S, V]) bool {
		tmp.K = append(tmp.K, k)
		tmp.S = append(tmp.S, item.S)
		tmp.V = append(tmp.V, item.V)
		return false
	})

	return json.Marshal(tmp)
}

// UnmarshalJSON
func (z *ZSet[K, S, V]) UnmarshalJSON(src []byte) error {
	var tmp zsetJSON[K, S, V]
	if err := json.Unmarshal(src, &tmp); err != nil {
		return err
	}

	for i, k := range tmp.K {
		z.SetWithScore(k, tmp.S[i], tmp.V[i])
	}
	return nil
}
