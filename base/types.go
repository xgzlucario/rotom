package base

import (
	"golang.org/x/exp/constraints"
)

type Ordered constraints.Ordered

type Number interface {
	~int | ~int32 | ~int64 | ~uint | ~uint32 | ~uint64
}

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
	Always
)

// AppMode for run mode.
type AppMode byte

const (
	DefaultMode AppMode = iota // Run for pakage import
	ServerMode                 // Run server and listen
)
