// Package aof implements the append-only file.
package aof

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/tidwall/redcon"
)

// FsyncPolicy mirrors redis `appendfsync`.
type FsyncPolicy int

const (
	FsyncEverySec FsyncPolicy = iota
	FsyncAlways
	FsyncNo
)

func (a *Aof) Policy() FsyncPolicy { return a.policy }

func ParseFsyncPolicy(s string) (FsyncPolicy, error) {
	switch s {
	case "", "everysec":
		return FsyncEverySec, nil
	case "always":
		return FsyncAlways, nil
	case "no":
		return FsyncNo, nil
	}
	return 0, fmt.Errorf("invalid appendfsync %q, want always|everysec|no", s)
}

// Aof manages an append-only file.
//
// Commands are buffered in memory by Append, written to the file once per
// event loop iteration by Write (before replies are sent), and fsynced
// according to the policy.
type Aof struct {
	file   *os.File
	buf    *bytes.Buffer
	policy FsyncPolicy
}

func NewAof(path string, policy FsyncPolicy) (*Aof, error) {
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return &Aof{
		file:   fd,
		buf:    bytes.NewBuffer(make([]byte, 0, 4096)),
		policy: policy,
	}, nil
}

func (a *Aof) Close() error {
	if err := a.Flush(); err != nil {
		return err
	}
	return a.file.Close()
}

// AppendRaw buffers an already RESP-encoded command.
func (a *Aof) AppendRaw(raw []byte) {
	a.buf.Write(raw)
}

// Append buffers a command encoded as a RESP array.
func (a *Aof) Append(argv [][]byte) {
	a.buf.Write(Encode(a.buf.AvailableBuffer(), argv))
}

// Encode appends argv to b as a RESP array.
func Encode(b []byte, argv [][]byte) []byte {
	b = append(b, '*')
	b = strconv.AppendInt(b, int64(len(argv)), 10)
	b = append(b, '\r', '\n')
	for _, arg := range argv {
		b = append(b, '$')
		b = strconv.AppendInt(b, int64(len(arg)), 10)
		b = append(b, '\r', '\n')
		b = append(b, arg...)
		b = append(b, '\r', '\n')
	}
	return b
}

// Write moves buffered commands to the file, and fsyncs if the policy is always.
func (a *Aof) Write() error {
	if a.buf.Len() == 0 {
		return nil
	}
	if _, err := a.buf.WriteTo(a.file); err != nil {
		return err
	}
	if a.policy == FsyncAlways {
		return a.file.Sync()
	}
	return nil
}

// Sync fsyncs the file.
func (a *Aof) Sync() error { return a.file.Sync() }

// Flush writes buffered commands and fsyncs.
func (a *Aof) Flush() error {
	if err := a.Write(); err != nil {
		return err
	}
	return a.file.Sync()
}

// Read replays every command stored in the file.
func (a *Aof) Read(fn func(args [][]byte)) error {
	if _, err := a.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	// NOTE: rd.ReadCommands() only returns the commands of one buffered
	// chunk, so we must loop until EOF.
	rd := redcon.NewReader(a.file)
	for {
		cmd, err := rd.ReadCommand()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		fn(cmd.Args)
	}
}
