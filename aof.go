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

func (aof *Aof) Close() error {
	return aof.file.Close()
}

func (aof *Aof) Write(buf []byte) (int, error) {
	return aof.buf.Write(buf)
}

func (aof *Aof) Flush() error {
	_, _ = aof.buf.WriteTo(aof.file)
	return aof.file.Sync()
}

func (aof *Aof) Read(fn func(args []RESP)) error {
	// Read file data by mmap.
	data, err := mmap.MapFile(aof.file, false)
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
