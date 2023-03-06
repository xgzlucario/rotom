package structx

import "github.com/xgzlucario/rotom/base"

type zslNode[K, V base.Ordered] struct {
	key   K
	value V
}

type ZSet[K, V base.Ordered] struct {
	zsl *Skiplist[K, V]
	m   Map[K, *zslNode[K, V]]
}

// NewZSet
func NewZSet[K, V base.Ordered]() *ZSet[K, V] {
	return &ZSet[K, V]{
		zsl: NewSkipList[K, V](),
		m:   Map[K, *zslNode[K, V]]{},
	}
}

// Set: set key and value
func (z *ZSet[K, V]) Set(key K, value V) {
	n, ok := z.m[key]
	if ok {
		// value not change
		if value == n.value {
			return
		}
		n.value = value
		z.zsl.Delete(key, n.value)
		z.zsl.Add(key, n.value)
	} else {
		z.insert(key, value)
	}
}

// Incr: Increment value by key
func (z *ZSet[K, V]) Incr(key K, value V) V {
	n, ok := z.m[key]
	// not exist
	if !ok {
		z.insert(key, value)
		return value
	}
	// exist
	z.zsl.Delete(key, n.value)
	n.value += value
	z.zsl.Add(key, n.value)

	return n.value
}

// Delete: delete keys
func (z *ZSet[K, V]) Delete(keys ...K) error {
	for _, key := range keys {
		n, ok := z.m[key]
		if !ok {
			return base.ErrKeyNotFound(key)
		}
		z.delete(n.key, n.value)
	}
	return nil
}

// GetScoreWithRank
func (z *ZSet[K, V]) GetScoreWithRank(key K) (V, int, error) {
	return z.zsl.GetScoreWithRank(key)
}

// GetByRank: get value by rank
func (z *ZSet[K, V]) GetByRank(rank int) (k K, v V, err error) {
	if rank < 0 || rank > z.Len() {
		return k, v, base.ErrOutOfBounds(rank)
	}
	return z.zsl.GetByRank(rank)
}

// GetScore
func (z *ZSet[K, V]) GetScore(key K) (v V, err error) {
	node, ok := z.m[key]
	if !ok {
		return v, base.ErrKeyNotFound(key)
	}
	return node.value, nil
}

// Copy
func (z *ZSet[K, V]) Copy() *ZSet[K, V] {
	newZSet := NewZSet[K, V]()
	z.Range(0, -1, func(key K, value V) bool {
		newZSet.Set(key, value)
		return false
	})
	return z
}

// Union
func (z *ZSet[K, V]) Union(target *ZSet[K, V]) {
	target.Range(0, -1, func(key K, value V) bool {
		z.Incr(key, value)
		return false
	})
}

// Range
func (z *ZSet[K, V]) Range(start, end int, f func(K, V) bool) {
	z.zsl.Range(start, end, f)
}

// RangeByScores
func (z *ZSet[K, V]) RangeByScores(min, max V, f func(K, V) bool) {
	z.zsl.RangeByScores(min, max, f)
}

func (z *ZSet[K, V]) Len() int {
	return len(z.m)
}

// make sure that key is not exist!
func (z *ZSet[K, V]) insert(key K, value V) *skiplistNode[K, V] {
	z.m[key] = &zslNode[K, V]{
		key:   key,
		value: value,
	}
	return z.zsl.Add(key, value)
}

// make sure that key exist!
func (z *ZSet[K, V]) delete(key K, value V) {
	delete(z.m, key)
	z.zsl.Delete(key, value)
}

// Print
func (z *ZSet[K, V]) Print() {
	z.zsl.Print()
}

// marshal type
type zsetJSON[K, V base.Ordered] struct {
	K []K
	V []V
}

func (z *ZSet[K, V]) MarshalJSON() ([]byte, error) {
	tmp := zsetJSON[K, V]{
		K: make([]K, 0, len(z.m)),
		V: make([]V, 0, len(z.m)),
	}
	for key, node := range z.m {
		tmp.K = append(tmp.K, key)
		tmp.V = append(tmp.V, node.value)
	}
	return base.MarshalJSON(tmp)
}

func (z *ZSet[K, V]) UnmarshalJSON(src []byte) error {
	var tmp zsetJSON[K, V]
	if err := base.UnmarshalJSON(src, &tmp); err != nil {
		return err
	}

	for i, k := range tmp.K {
		z.insert(k, tmp.V[i])
	}
	return nil
}
