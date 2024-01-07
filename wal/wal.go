package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/rosedblabs/wal"
)

// Log is a cocurrent unsafe write ahead log.
type Log struct {
	dirPath       string
	log           *wal.WAL
	pendingWrites []byte
}

// Open create a log dir for write ahead log.
func Open(dirPath string) (*Log, error) {
	options := wal.DefaultOptions
	options.DirPath = dirPath

	log, err := wal.Open(options)
	if err != nil {
		return nil, err
	}
	return &Log{dirPath: dirPath, log: log}, nil
}

// Write
func (l *Log) Write(data []byte) {
	// append length
	l.pendingWrites = binary.AppendUvarint(l.pendingWrites, uint64(len(data)))
	// append data
	l.pendingWrites = append(l.pendingWrites, data...)
}

// Sync
func (l *Log) Sync() error {
	if len(l.pendingWrites) == 0 {
		return nil
	}
	// comrpess data
	buf := compress(l.pendingWrites, nil)
	_, err := l.log.Write(buf)
	if err != nil {
		return err
	}
	l.pendingWrites = l.pendingWrites[:0]

	return l.log.Sync()
}

// Range
func (l *Log) Range(f func([]byte) error) error {
	// iterate all data in wal
	reader := l.log.NewReader()
	var data []byte

	for {
		val, _, err := reader.Next()
		if err == io.EOF {
			break
		}
		// decompress data
		val, err = decompress(val, nil)
		if err != nil {
			return err
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

// ActiveSegmentID
func (l *Log) ActiveSegmentID() uint32 {
	return l.log.ActiveSegmentID()
}

// OpenNewActiveSegment
func (l *Log) OpenNewActiveSegment() error {
	return l.log.OpenNewActiveSegment()
}

// RemoveOldSegments remove all segments which is less than maxSegmentID.
func (l *Log) RemoveOldSegments(maxSegmentID uint32) error {
	maxSegmentName := fmt.Sprintf("%09d", maxSegmentID)

	filepath.WalkDir(l.dirPath, func(path string, file os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if file.Name() < maxSegmentName {
			os.Remove(path)
		}
		return nil
	})
	return nil
}

// Close
func (l *Log) Close() error {
	return l.log.Close()
}
