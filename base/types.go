package base

import (
	"golang.org/x/exp/constraints"
)

type Ordered constraints.Ordered

type Marshaler interface {
	MarshalJSON() ([]byte, error)
	UnmarshalJSON([]byte) error
}

type Binarier interface {
	MarshalBinary() ([]byte, error)
	UnmarshalBinary([]byte) error
}

type GTreeJSON[K, V any] struct {
	K []K
	V []V
}

type Raw []byte

// SyncPolicy represents how often data is synced to disk.
type SyncPolicy byte

const (
	Never SyncPolicy = iota
	EverySecond
	Always
)
