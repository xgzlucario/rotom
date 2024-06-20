package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAof(t *testing.T) {
	assert := assert.New(t)
	setCommand := []byte("*3\r\n$3\r\nset\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")

	t.Run("write", func(t *testing.T) {
		aof, err := NewAof("test.aof")
		assert.Nil(err)
		defer aof.Close()

		aof.Flush()
		aof.Write(setCommand)
		aof.Flush()
	})

	t.Run("read", func(t *testing.T) {
		aof, err := NewAof("test.aof")
		assert.Nil(err)
		defer aof.Close()
	})

	t.Run("read-error", func(t *testing.T) {
		aof, _ := NewAof("not-exist.aof")
		defer aof.Close()

		aof.Read(func(args []RESP) {
			panic("should not call")
		})
	})
}
