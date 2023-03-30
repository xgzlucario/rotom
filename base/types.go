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

type Texter interface {
	MarshalText() ([]byte, error)
	UnmarshalText([]byte) error
}

type GTreeJSON[K, V any] struct {
	K []K
	V []V
}

type Raw []byte
