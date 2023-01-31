package base

import (
	"golang.org/x/exp/constraints"
)

type Value constraints.Ordered

type Stringer interface {
	any
	String() string
}

type Marshaler interface {
	any
	MarshalJSON() ([]byte, error)
	UnmarshalJSON([]byte) error
}
