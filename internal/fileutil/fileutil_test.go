package fileutil

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteAtomic(t *testing.T) {
	ast := assert.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "f")
	ast.Nil(WriteAtomic(path, []byte("one")))
	ast.Nil(WriteAtomic(path, []byte("two")))
	data, err := os.ReadFile(path)
	ast.Nil(err)
	ast.Equal("two", string(data))
	_, err = os.Stat(path + ".tmp")
	ast.True(os.IsNotExist(err))

	ast.NotNil(WriteAtomic(filepath.Join(dir, "missing", "f"), nil))
	ast.NotNil(WriteAtomic(dir, nil)) // rename over a directory fails
	ast.NotNil(SyncDir(filepath.Join(dir, "missing")))
}
