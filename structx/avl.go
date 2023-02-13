package structx

import (
	"github.com/xgzlucario/rotom/base"
	"github.com/zyedidia/generic/avl"
)

// AVLTree
type AVLTree[K base.Ordered, V any] struct {
	*avl.Tree[K, V]
}

// NewAVLTree
func NewAVLTree[K base.Ordered, V any]() *AVLTree[K, V] {
	return &AVLTree[K, V]{
		Tree: avl.New[K, V](func(a, b K) bool { return a < b }),
	}
}

// marshal type
type avltreeJSON[K base.Ordered, V any] struct {
	K []K
	V []V
}

func (b *AVLTree[K, V]) MarshalJSON() ([]byte, error) {
	tmp := avltreeJSON[K, V]{
		K: make([]K, 0, b.Size()),
		V: make([]V, 0, b.Size()),
	}

	b.Each(func(key K, val V) {
		tmp.K = append(tmp.K, key)
		tmp.V = append(tmp.V, val)
	})
	return base.MarshalJSON(tmp)
}

func (b *AVLTree[K, V]) UnmarshalJSON(src []byte) error {
	var tmp avltreeJSON[K, V]
	if err := base.UnmarshalJSON(src, b); err != nil {
		return err
	}

	for i, k := range tmp.K {
		b.Put(k, tmp.V[i])
	}
	return nil
}
