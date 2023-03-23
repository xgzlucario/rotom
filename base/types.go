package base

import (
	"golang.org/x/exp/constraints"
)

type Ordered constraints.Ordered

type Stringer interface {
	String() string
}

type Marshaler interface {
	MarshalJSON() ([]byte, error)
	UnmarshalJSON([]byte) error
}

type GTreeJSON[K Ordered, V any] struct {
	K []K
	V []V
}
