package structx

import "github.com/xgzlucario/rotom/base"

// ZSet
type ZSet[K, S base.Ordered, V any] struct {
	data Map[K, *znode[S, V]]
	tree *RBTree[S, K]
}

type znode[S base.Ordered, V any] struct {
	S S
	V V
}

type ziter[S base.Ordered, V any] struct {
	n *rbnode[S, V]
}

// NewZSet
func NewZSet[K, S base.Ordered, V any]() *ZSet[K, S, V] {
	return &ZSet[K, S, V]{
		data: NewMap[K, *znode[S, V]](),
		tree: NewRBTree[S, K](),
	}
}

// Get
func (z *ZSet[K, S, V]) Get(key K) (V, S, bool) {
	item, ok := z.data.Get(key)
	if !ok {
		var v V
		var s S
		return v, s, false
	}
	return item.V, item.S, ok
}

// Set upsert value by key
func (z *ZSet[K, S, V]) Set(key K, value V) {
	item, ok := z.data.Get(key)
	if ok {
		item.V = value

	} else {
		item = &znode[S, V]{V: value}
		z.data.Put(key, item)
		z.tree.Insert(item.S, key)
	}
}

// SetScore upsert score by key
func (z *ZSet[K, S, V]) SetScore(key K, score S) {
	item, ok := z.data.Get(key)
	if ok {
		z.updateScore(item, key, score)

	} else {
		z.data.Put(key, &znode[S, V]{S: score})
		z.tree.Insert(score, key)
	}
}

// update score of key
func (z *ZSet[K, S, V]) updateScore(node *znode[S, V], key K, score S) {
	z.tree.Delete(node.S)
	node.S = score
	z.tree.Insert(score, key)
}

// SetWithScore upsert value and score by key
func (z *ZSet[K, S, V]) SetWithScore(key K, score S, value V) {
	item, ok := z.data.Get(key)
	if ok {
		item.V = value
		z.updateScore(item, key, score)

	} else {
		z.data.Put(key, &znode[S, V]{S: score, V: value})
		z.tree.Insert(score, key)
	}
}

// Incr
func (z *ZSet[K, S, V]) Incr(key K, score S) S {
	item, ok := z.data.Get(key)
	if ok {
		z.updateScore(item, key, item.S+score)
		return item.S

	} else {
		z.data.Put(key, &znode[S, V]{S: score})
		z.tree.Insert(score, key)
		return score
	}
}

// Delete
func (z *ZSet[K, S, V]) Delete(key K) (v V, ok bool) {
	item, ok := z.data.Get(key)
	if ok {
		z.data.Delete(key)
		z.tree.Delete(item.S)
		return item.V, ok
	}
	return
}

// Size
func (z *ZSet[K, S, V]) Size() int {
	return z.tree.size
}

// Iter return an iterator by score ASC
func (z *ZSet[K, S, V]) Iter() *ziter[S, K] {
	return &ziter[S, K]{z.tree.Iterator()}
}

// Score
func (z *ziter[S, K]) Score() S {
	return z.n.Key
}

// Key
func (z *ziter[S, K]) Key() K {
	return z.n.Value
}

// Valid
func (z *ziter[S, K]) Valid() bool {
	return z.n != nil
}

// Next
func (z *ziter[S, K]) Next() {
	z.n = z.n.Next()
}

// MarshalJSON
func (z *ZSet[K, S, V]) MarshalJSON() ([]byte, error) {
	return z.data.MarshalJSON()
}

// UnmarshalJSON
func (z *ZSet[K, S, V]) UnmarshalJSON(src []byte) error {
	if err := z.data.UnmarshalJSON(src); err != nil {
		return err
	}

	z.data.Iter(func(k K, item *znode[S, V]) bool {
		z.tree.Insert(item.S, k)
		return false
	})

	return nil
}
