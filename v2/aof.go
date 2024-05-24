package main

import (
	"bytes"
	"io"
	"os"
	"sync"
	"time"
)

// Aof manages an append-only file system for storing data.
type Aof struct {
	file *os.File
	buf  *bytes.Buffer
	mu   sync.Mutex
}

func NewAof(path string) (*Aof, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	aof := &Aof{
		file: f,
		buf:  bytes.NewBuffer(make([]byte, 0, 1024)),
	}

	go func() {
		ticker := time.NewTicker(time.Second)
		for range ticker.C {
			aof.mu.Lock()
			// flush buffer to disk
			aof.file.Write(aof.buf.Bytes())
			aof.buf.Reset()
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

func (aof *Aof) Write(value Value) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	_, err := aof.buf.Write(value.Marshal())
	return err
}

func (aof *Aof) Read(fn func(value Value)) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	// Ensure the file pointer is at the start.
	_, err := aof.file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	reader := NewResp(aof.file)

	// Iterate over the records in the file, applying the function to each.
	for {
		value, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		fn(value)
	}

	return nil
}
