package base

import (
	"golang.org/x/exp/constraints"
)

type Ordered constraints.Ordered

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

// Writer
type Writer interface {
	WriteString(string) error
	WriteByte(byte) error
	Write([]byte) error
}

// SyncPolicy represents how often data is synced to disk.
type SyncPolicy byte

const (
	Never SyncPolicy = iota
	EverySecond
	// TODO: Sync
)
