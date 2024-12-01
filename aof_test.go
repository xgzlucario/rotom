package main

import (
	"github.com/tidwall/redcon"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAof(t *testing.T) {
	ast := assert.New(t)
	cmdStr := []byte("*3\r\n$3\r\nset\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")

	t.Run("write", func(t *testing.T) {
		aof, err := NewAof("test.aof")
		ast.Nil(err)
		defer aof.Close()

		_ = aof.Flush()
		_, _ = aof.Write(cmdStr)
		_ = aof.Flush()
	})

	t.Run("read", func(t *testing.T) {
		aof, err := NewAof("test.aof")
		ast.Nil(err)
		_ = aof.Read(func(args []redcon.RESP) {
			// SET foo bar
			ast.Equal(len(args), 3)
			ast.Equal(args[0].String(), "set")
			ast.Equal(args[1].String(), "foo")
			ast.Equal(args[2].String(), "bar")
		})
		defer aof.Close()
	})

	t.Run("read-err-content", func(t *testing.T) {
		aof, _ := NewAof("LICENSE")
		err := aof.Read(func(args []redcon.RESP) {})
		ast.NotNil(err)
	})

	t.Run("empty-aof", func(t *testing.T) {
		aof, _ := NewAof("not-exist.aof")
		defer aof.Close()

		_ = aof.Read(func(args []redcon.RESP) {
			panic("should not call")
		})
	})

	t.Run("read-err-fileType", func(t *testing.T) {
		_, err := NewAof("internal")
		ast.NotNil(err)
	})
}
