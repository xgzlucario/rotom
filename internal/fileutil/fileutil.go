// Package fileutil holds small durable file helpers.
package fileutil

import (
	"os"
	"path/filepath"
)

// WriteAtomic writes data to path so that a crash leaves either the old file
// or the complete new one: write a temp file, fsync it, rename it over path,
// then fsync the directory so that the rename itself is durable.
func WriteAtomic(path string, data []byte) (err error) {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
		}
	}()
	if _, err = f.Write(data); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	return SyncDir(filepath.Dir(path))
}

// SyncDir fsyncs a directory, making renames and deletions in it durable.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
