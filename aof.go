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
)

// Aof manages an append-only file system for storing data.
type Aof struct {
	filePath string
	file     *os.File
	buf      *bytes.Buffer
}

func NewAof(path string) (*Aof, error) {
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return &Aof{
		file:     fd,
		filePath: path,
		buf:      bytes.NewBuffer(make([]byte, 0, KB)),
	}, nil
}

func (aof *Aof) Close() error {
	return aof.file.Close()
}

func (aof *Aof) Write(buf []byte) (int, error) {
	return aof.buf.Write(buf)
}

func (aof *Aof) Flush() error {
	if aof.buf.Len() == 0 {
		return nil
	}
	aof.buf.WriteTo(aof.file)
	return aof.file.Sync()
}

func (aof *Aof) Read(fn func(args []Arg)) error {
	// Read file data by mmap.
	data, err := mmap.Open(aof.filePath, false)
	if len(data) == 0 {
		return nil
	}
	if err != nil {
		return err
	}

	// Iterate over the records in the file, applying the function to each.
	reader := NewResp(data)
	argsBuf := make([]Arg, 3)
	for {
		values, err := reader.ReadNextCommand(argsBuf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		fn(values)
	}

	return nil
}
