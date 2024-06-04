package main

import (
	"bytes"
	"io"
	"os"
	"sync"
	"time"

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
	mu       sync.Mutex
}

func NewAof(path string) (*Aof, error) {
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	aof := &Aof{
		file:     fd,
		filePath: path,
		buf:      bytes.NewBuffer(make([]byte, 0, MB)),
	}

	go func() {
		ticker := time.NewTicker(time.Second)
		for range ticker.C {
			aof.mu.Lock()
			// flush buffer to disk
			aof.buf.WriteTo(aof.file)
			aof.file.Sync()
			aof.mu.Unlock()
		}
	}()

	return aof, nil
}

func (aof *Aof) Close() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	return aof.file.Close()
}

func (aof *Aof) Write(buf []byte) error {
	aof.mu.Lock()
	_, err := aof.buf.Write(buf)
	aof.mu.Unlock()
	return err
}

func (aof *Aof) Read(fn func(value Value)) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

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
	var input Value
	for {
		err := reader.Read(&input)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		fn(input)
	}

	return nil
}
