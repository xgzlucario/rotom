package aof

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAof(t *testing.T) {
	ast := assert.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "test.aof")

	t.Run("write-read", func(t *testing.T) {
		aof, err := NewAof(path, FsyncEverySec)
		ast.Nil(err)
		aof.AppendRaw([]byte("*3\r\n$3\r\nset\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))
		aof.Append([][]byte{[]byte("del"), []byte("foo")})
		ast.Nil(aof.Close())

		aof, err = NewAof(path, FsyncEverySec)
		ast.Nil(err)
		defer aof.Close()
		var got [][]string
		ast.Nil(aof.Read(func(args [][]byte) {
			var cmd []string
			for _, a := range args {
				cmd = append(cmd, string(a))
			}
			got = append(got, cmd)
		}))
		ast.Equal([][]string{{"set", "foo", "bar"}, {"del", "foo"}}, got)
	})

	t.Run("read-many", func(t *testing.T) {
		// more than one read buffer: redcon's ReadCommands would stop early.
		p := filepath.Join(dir, "many.aof")
		aof, _ := NewAof(p, FsyncNo)
		for range 10000 {
			aof.Append([][]byte{[]byte("incr"), []byte("counter")})
		}
		ast.Nil(aof.Close())

		aof, _ = NewAof(p, FsyncNo)
		defer aof.Close()
		n := 0
		ast.Nil(aof.Read(func([][]byte) { n++ }))
		ast.Equal(10000, n)
	})

	t.Run("read-err-content", func(t *testing.T) {
		p := filepath.Join(dir, "bad.aof")
		ast.Nil(os.WriteFile(p, []byte("*1\r\n$abc\r\n"), 0644))
		aof, _ := NewAof(p, FsyncNo)
		defer aof.Close()
		ast.NotNil(aof.Read(func([][]byte) {}))
	})

	t.Run("empty-aof", func(t *testing.T) {
		aof, _ := NewAof(filepath.Join(dir, "empty.aof"), FsyncNo)
		defer aof.Close()
		ast.Nil(aof.Read(func([][]byte) { panic("should not call") }))
	})

	t.Run("read-err-fileType", func(t *testing.T) {
		_, err := NewAof(dir, FsyncNo)
		ast.NotNil(err)
	})

	t.Run("parse-policy", func(t *testing.T) {
		p, err := ParseFsyncPolicy("always")
		ast.Nil(err)
		ast.Equal(FsyncAlways, p)
		_, err = ParseFsyncPolicy("sometimes")
		ast.NotNil(err)
	})
}
