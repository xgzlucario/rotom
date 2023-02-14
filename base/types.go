package base

import (
	"time"

	"golang.org/x/exp/constraints"
)

type Ordered constraints.Ordered

type Bases interface {
	Ordered | ~bool | time.Time
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

type GTreeJSON[K Ordered, V any] struct {
	K []K
	V []V
}
