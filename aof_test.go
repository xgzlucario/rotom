package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAof(t *testing.T) {
	ast := assert.New(t)
	setCommand := []byte("*3\r\n$3\r\nset\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")

	t.Run("write", func(t *testing.T) {
		aof, err := NewAof("test.aof")
		ast.Nil(err)
		defer aof.Close()

		_ = aof.Flush()
		_, _ = aof.Write(setCommand)
		_ = aof.Flush()
	})

	t.Run("read", func(t *testing.T) {
		aof, err := NewAof("test.aof")
		ast.Nil(err)

		_ = aof.Read(func(args []RESP) {
			// SET foo bar
			ast.Equal(len(args), 3)
			ast.Equal(args[0].ToString(), "set")
			ast.Equal(args[1].ToString(), "foo")
			ast.Equal(args[2].ToString(), "bar")
		})

		defer aof.Close()
	})

	t.Run("empty-aof", func(t *testing.T) {
		aof, _ := NewAof("not-exist.aof")
		defer aof.Close()

		_ = aof.Read(func(args []RESP) {
			panic("should not call")
		})
	})

	t.Run("read-wrong-file", func(t *testing.T) {
		_, err := NewAof("internal")
		ast.NotNil(err)
	})
}
