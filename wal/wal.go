package wal

import (
	"encoding/binary"
	"io"
	"sync"

	"github.com/rosedblabs/wal"
)

type Log struct {
	mu              sync.RWMutex
	log             *wal.WAL
	enabledCompress bool
	pendingWrites   []byte
}

// Open create a log dir for write ahead log.
func Open(dirPath string) (*Log, error) {
	options := wal.DefaultOptions
	options.DirPath = dirPath

	log, err := wal.Open(options)
	if err != nil {
		return nil, err
	}
	return &Log{log: log}, nil
}

// Write
func (l *Log) Write(data []byte) {
	l.mu.Lock()

	// append length
	l.pendingWrites = binary.AppendUvarint(l.pendingWrites, uint64(len(data)))
	// append data
	l.pendingWrites = append(l.pendingWrites, data...)

	l.mu.Unlock()
}

// Sync
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.pendingWrites) == 0 {
		return nil
	}

	buf := l.pendingWrites
	// if compress enabled
	if l.enabledCompress {
		buf = compress(buf, nil)
	}

	_, err := l.log.Write(buf)
	if err != nil {
		return err
	}
	l.pendingWrites = l.pendingWrites[:0]

	return l.log.Sync()
}

// Range
func (l *Log) Range(f func([]byte) error) error {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// iterate all data in wal
	reader := l.log.NewReader()
	var data []byte

	for {
		val, _, err := reader.Next()
		if err == io.EOF {
			break
		}

		// if compress enabled
		if l.enabledCompress {
			val, err = decompress(val, nil)
			if err != nil {
				return err
			}
		}

		index := 0
		for index < len(val) {
			// read length
			len, n := binary.Uvarint(val[index:])
			index += n
			// read data
			data = val[index : index+int(len)]
			index += int(len)

			if err := f(data); err != nil {
				return err
			}
		}
	}

	return nil
}

// Close
func (l *Log) Close() error {
	return l.log.Close()
}

// SetEnabledCompress
func (l *Log) SetEnabledCompress(v bool) {
	l.enabledCompress = v
}
