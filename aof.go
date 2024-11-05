package main

import (
	"bytes"
	"io"
	"os"

	"github.com/tidwall/mmap"
)

const (
	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB
)

// Aof manages an append-only file system for storing data.
type Aof struct {
	file *os.File
	buf  *bytes.Buffer
}

func NewAof(path string) (*Aof, error) {
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return &Aof{
		file: fd,
		buf:  bytes.NewBuffer(make([]byte, 0, KB)),
	}, nil
}

func (a *Aof) Close() error {
	return a.file.Close()
}

func (a *Aof) Write(buf []byte) (int, error) {
	return a.buf.Write(buf)
}

func (a *Aof) Flush() error {
	_, _ = a.buf.WriteTo(a.file)
	return a.file.Sync()
}

func (a *Aof) Read(fn func(args []RESP)) error {
	// Read file data by mmap.
	data, err := mmap.MapFile(a.file, false)
	if len(data) == 0 {
		return nil
	}
	if err != nil {
		return err
	}

	// Iterate over the records in the file, applying the function to each.
	reader := NewReader(data)
	argsBuf := make([]RESP, 8)
	for {
		args, _, err := reader.ReadNextCommand(argsBuf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		fn(args)
	}

	return nil
}
