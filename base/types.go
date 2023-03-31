package base

import (
	"golang.org/x/exp/constraints"
)

type Ordered constraints.Ordered

type Marshaler interface {
	MarshalJSON() ([]byte, error)
	UnmarshalJSON([]byte) error
}

type GTreeJSON[K, V any] struct {
	K []K
	V []V
}
