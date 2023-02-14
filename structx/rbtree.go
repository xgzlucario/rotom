package structx

import (
	rbtree "github.com/sakeven/RbTree"
	"github.com/xgzlucario/rotom/base"
)

// RBTree
type RBTree[K base.Ordered, V any] struct {
	*rbtree.Tree[K, V]
}

// NewRBTree
func NewRBTree[K base.Ordered, V any]() *RBTree[K, V] {
	return &RBTree[K, V]{
		Tree: rbtree.NewTree[K, V](),
	}
}

// marshal type
type gtreeJSON[K base.Ordered, V any] struct {
	K []K
	V []V
}

func (b *RBTree[K, V]) MarshalJSON() ([]byte, error) {
	tmp := gtreeJSON[K, V]{
		K: make([]K, 0, b.Size()),
		V: make([]V, 0, b.Size()),
	}

	for f := b.Iterator(); f != nil; f = f.Next() {
		tmp.K = append(tmp.K, f.Key)
		tmp.V = append(tmp.V, f.Value)
	}
	return base.MarshalJSON(tmp)
}

func (b *RBTree[K, V]) UnmarshalJSON(src []byte) error {
	var tmp gtreeJSON[K, V]
	if err := base.UnmarshalJSON(src, b); err != nil {
		return err
	}

	for i, k := range tmp.K {
		b.Insert(k, tmp.V[i])
	}
	return nil
}
