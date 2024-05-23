package main

import (
	"bufio"
	"io"
	"os"
	"sync"
	"time"
)

// Aof manages an append-only file system for storing data.
type Aof struct {
	file *os.File      // File handle for the AOF file.
	rd   *bufio.Reader // Buffered reader for reading the AOF file.
	mu   sync.Mutex    // Mutex to protect file operations.
}

// NewAof opens or creates an append-only file and starts a background process for syncing data.
func NewAof(path string) (*Aof, error) {
	// Open the file with both read and write permissions, creating it if it does not exist.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	aof := &Aof{
		file: f,
		rd:   bufio.NewReader(f),
	}

	// Start a goroutine to periodically sync the file to disk.
	go func() {
		ticker := time.NewTicker(time.Second)
		for range ticker.C {
			aof.mu.Lock()
			aof.file.Sync()
			aof.mu.Unlock()
		}
	}()

	return aof, nil
}

// Close safely closes the AOF file.
func (aof *Aof) Close() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	return aof.file.Close()
}

// Write writes a marshaled value to the AOF file.
func (aof *Aof) Write(value Value) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	_, err := aof.file.Write(value.Marshal())
	return err
}

// Read reads entries from the AOF file and applies a function to each.
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
