package structx

import "github.com/xgzlucario/rotom/base"

// ZSet structure: K key, S score, V value
type ZSet[K, S base.Ordered, V any] struct {
	zero S

	// based on map
	data Map[K, *zsNode[S, V]]

	// based on red-black tree
	tree *RBTree[S, K]
}

type zsNode[S base.Ordered, V any] struct {
	S S
	V V
}

type zsIter[S base.Ordered, V any] struct {
	n *rbnode[S, V]
}

// NewZSet return zset with specific types of [key, score, value]
func NewZSet[K, S base.Ordered, V any]() *ZSet[K, S, V] {
	return &ZSet[K, S, V]{
		tree: NewRBTree[S, K](),
		data: Map[K, *zsNode[S, V]]{},
	}
}

// Get return value and score by key
func (z *ZSet[K, S, V]) Get(key K) (V, S, bool) {
	item, ok := z.data[key]
	if !ok {
		var v V
		var s S
		return v, s, false
	}
	return item.V, item.S, ok
}

// Set upsert value by key
func (z *ZSet[K, S, V]) Set(key K, value V) {
	item, ok := z.data[key]
	if ok {
		item.V = value

	} else {
		z.data[key] = &zsNode[S, V]{V: value}
	}
}

// SetScore upsert score by key
func (z *ZSet[K, S, V]) SetScore(key K, score S) {
	item, ok := z.data[key]
	if ok {
		z.updateScore(item, key, score)

	} else {
		z.data[key] = &zsNode[S, V]{S: score}
		// score is not zero
		if score != z.zero {
			z.tree.Insert(score, key)
		}
	}
}

func (z *ZSet[K, S, V]) updateScore(node *zsNode[S, V], key K, score S) {
	// score no change
	if node.S == score {
		return
	}
	// score is not zero value
	if node.S != z.zero {
		z.tree.Delete(node.S)
	}
	node.S = score
	if score != z.zero {
		z.tree.Insert(score, key)
	}
}

// SetWithScore upsert value and score by key
func (z *ZSet[K, S, V]) SetWithScore(key K, score S, value V) {
	item, ok := z.data[key]
	if ok {
		item.V = value
		z.updateScore(item, key, score)

	} else {
		z.data[key] = &zsNode[S, V]{S: score, V: value}
		// score is not zero
		if score != z.zero {
			z.tree.Insert(score, key)
		}
	}
}

// Incr: Increment key by score
func (z *ZSet[K, S, V]) Incr(key K, score S) S {
	item, ok := z.data[key]
	if ok {
		z.updateScore(item, key, item.S+score)
		return item.S + score

	} else {
		z.data[key] = &zsNode[S, V]{S: score}
		// score is not zero
		if score != z.zero {
			z.tree.Insert(score, key)
		}
		return score
	}
}

// Delete
func (z *ZSet[K, S, V]) Delete(key K) (v V, ok bool) {
	item, ok := z.data[key]
	if ok {
		delete(z.data, key)
		// score is not zero
		if item.S != z.zero {
			z.tree.Delete(item.S)
		}
		return v, ok
	}
	return
}

// Size
func (z *ZSet[K, S, V]) Size() int {
	return z.tree.size
}

// Iter return an iterator
func (z *ZSet[K, S, V]) Iter() *zsIter[S, K] {
	return &zsIter[S, K]{
		z.tree.Iterator(),
	}
}

// Score
func (z *zsIter[S, K]) Score() S {
	return z.n.Key
}

// Key
func (z *zsIter[S, K]) Key() K {
	return z.n.Value
}

// Next
func (z *zsIter[S, K]) Next() *zsIter[S, K] {
	z.n = z.n.Next()
	return z
}

func (z *ZSet[K, S, V]) MarshalJSON() ([]byte, error) {
	return base.MarshalJSON(nil)
}

func (z *ZSet[K, S, V]) UnmarshalJSON(src []byte) error {
	return nil
}
