package base

import (
	"golang.org/x/exp/constraints"
)

type Ordered constraints.Ordered

type Number interface {
	Integer | float64 | float32
}

type Integer interface {
	~int | ~int32 | ~int64 | ~uint | ~uint32 | ~uint64
}

type Jsoner interface {
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

// Writer
type Writer interface {
	Write([]byte) (int, error)
	WriteByte(byte) error
}

// SyncPolicy represents how often data is synced to disk.
type SyncPolicy byte

const (
	Never SyncPolicy = iota
	EveryInterval
)
