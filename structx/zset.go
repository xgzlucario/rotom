package structx

import "github.com/xgzlucario/rotom/base"

// ZSet structure: K key, S score, V value
type ZSet[K, S base.Ordered, V any] struct {
	// data based on map
	data Map[K, *zslNode[S, V]]

	// score based on red-black tree
	tree *RBTree[S, K]
}

type zslNode[S base.Ordered, V any] struct {
	S S
	V V
}

type zslIter[S base.Ordered, V any] struct {
	n *rbnode[S, V]
}

// NewZSet with specific types of key, score, value
func NewZSet[K, S base.Ordered, V any]() *ZSet[K, S, V] {
	return &ZSet[K, S, V]{
		tree: NewRBTree[S, K](),
		data: Map[K, *zslNode[S, V]]{},
	}
}

// Get return value and score by key
func (z *ZSet[K, S, V]) Get(key K) (V, S, bool) {
	item, ok := z.data[key]
	return item.V, item.S, ok
}

// get zslNode
func (z *ZSet[K, S, V]) getNode(key K) (*zslNode[S, V], bool) {
	item, ok := z.data[key]
	return item, ok
}

// Set update or upsert value by key
func (z *ZSet[K, S, V]) Set(key K, score S, value V) {
	item, ok := z.data[key]
	if ok {
		item.V = value

	} else {
		item = &zslNode[S, V]{S: score, V: value}
		z.data[key] = item
		z.tree.Insert(item.S, key)
	}
}

// Incr: Increment key by score
func (z *ZSet[K, S, V]) Incr(key K, score S) S {
	item, ok := z.data[key]
	if ok {
		z.tree.Delete(item.S)
		item.S += score

	} else {
		item = &zslNode[S, V]{S: score}
		z.data[key] = item
	}
	// insert
	z.tree.Insert(item.S, key)
	return score
}

// Delete: delete key-value
func (z *ZSet[K, S, V]) Delete(key K) (v V, ok bool) {
	item, ok := z.data[key]
	if ok {
		delete(z.data, key)
		z.tree.Delete(item.S)
		return v, ok
	}
	return
}

// Len
func (z *ZSet[K, S, V]) Len() int {
	return z.tree.size
}

// Iter return an iterator
func (z *ZSet[K, S, V]) Iter() *zslIter[S, K] {
	return &zslIter[S, K]{
		z.tree.Iterator(),
	}
}

// Score
func (z *zslIter[S, K]) Score() S {
	return z.n.Key
}

// Key
func (z *zslIter[S, K]) Key() K {
	return z.n.Value
}

// Next
func (z *zslIter[S, K]) Next() *zslIter[S, K] {
	z.n = z.n.Next()
	return z
}

func (z *ZSet[K, S, V]) MarshalJSON() ([]byte, error) {
	return base.MarshalJSON(nil)
}

func (z *ZSet[K, S, V]) UnmarshalJSON(src []byte) error {
	return nil
}
