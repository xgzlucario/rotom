package base

import (
	"golang.org/x/exp/constraints"
)

type Ordered constraints.Ordered

type Bases interface {
	Ordered | ~bool
}

type Stringer interface {
	any
	String() string
}

type Marshaler interface {
	any
	MarshalJSON() ([]byte, error)
	UnmarshalJSON([]byte) error
}
