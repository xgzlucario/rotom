package structx

import (
	"github.com/xgzlucario/rotom/base"
	"github.com/zyedidia/generic/btree"
)

// Btree
type Btree[K base.Ordered, V any] struct {
	*btree.Tree[K, V]
}

// NewBtree
func NewBtree[K base.Ordered, V any]() *Btree[K, V] {
	return &Btree[K, V]{
		btree.New[K, V](func(a, b K) bool {
			return a < b
		}),
	}
}

// marshal type
type btreeJSON[K, V any] struct {
	K []K
	V []V
}

func (b *Btree[K, V]) MarshalJSON() ([]byte, error) {
	tmp := btreeJSON[K, V]{
		K: make([]K, 0, b.Size()),
		V: make([]V, 0, b.Size()),
	}

	b.Each(func(key K, val V) {
		tmp.K = append(tmp.K, key)
		tmp.V = append(tmp.V, val)
	})
	return base.MarshalJSON(tmp)
}

func (b *Btree[K, V]) UnmarshalJSON(src []byte) error {
	var tmp btreeJSON[K, V]
	if err := base.UnmarshalJSON(src, b); err != nil {
		return err
	}

	for i, k := range tmp.K {
		b.Put(k, tmp.V[i])
	}
	return nil
}
