package base

import (
	"bytes"

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

// CWriter
type CWriter struct {
	*bytes.Buffer
}

func (w *CWriter) Write(b []byte) error {
	_, err := w.Buffer.Write(b)
	return err
}

func (w *CWriter) WriteByte(b byte) error {
	return w.Buffer.WriteByte(b)
}

func (w *CWriter) WriteString(s string) error {
	_, err := w.Buffer.WriteString(s)
	return err
}

// NullWriter
type NullWriter struct{}

func (NullWriter) Write([]byte) error {
	return nil
}

func (NullWriter) WriteByte(byte) error {
	return nil
}

func (NullWriter) WriteString(string) error {
	return nil
}

// SyncPolicy represents how often data is synced to disk.
type SyncPolicy byte

const (
	Never SyncPolicy = iota
	EverySecond
	// TODO: Sync
)
