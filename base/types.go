package base

import (
	"golang.org/x/exp/constraints"
)

type Ordered constraints.Ordered

type Stringer interface {
	any
	String() string
}

type Marshaler interface {
	any
	MarshalJSON() ([]byte, error)
	UnmarshalJSON([]byte) error
}

type GTreeJSON[K Ordered, V any] struct {
	K []K
	V []V
}
