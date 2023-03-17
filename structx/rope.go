package structx

import "github.com/zyedidia/generic/rope"

type Rope[V any] struct {
	*rope.Node[V]
}

func NewRope[V any]() *Rope[V] {
	return &Rope[V]{rope.New[V](nil)}
}
