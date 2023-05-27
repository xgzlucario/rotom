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

type Gober interface {
	GobEncode() ([]byte, error)
	GobDecode([]byte) error
}

type GTreeJSON[K, V any] struct {
	K []K
	V []V
}

// SyncPolicy represents how often data is synced to disk.
type SyncPolicy byte

const (
	Never SyncPolicy = iota
	EverySecond
)
