package main

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriter(t *testing.T) {
	assert := assert.New(t)
	writer := NewWriter(16)

	t.Run("string", func(t *testing.T) {
		writer.WriteString("OK")
		assert.Equal(writer.b.String(), "+OK\r\n")
		writer.Reset()
	})

	t.Run("error", func(t *testing.T) {
		writer.WriteError(errors.New("err message"))
		assert.Equal(writer.b.String(), "-err message\r\n")
		writer.Reset()
	})

	t.Run("bulk", func(t *testing.T) {
		writer.WriteBulk([]byte("hello"))
		assert.Equal(writer.b.String(), "$5\r\nhello\r\n")
		writer.Reset()

		writer.WriteBulkString("world")
		assert.Equal(writer.b.String(), "$5\r\nworld\r\n")
		writer.Reset()
	})

	t.Run("integer", func(t *testing.T) {
		writer.WriteInteger(5)
		assert.Equal(writer.b.String(), ":5\r\n")
		writer.Reset()
	})
}

func TestReader(t *testing.T) {
	assert := assert.New(t)

	t.Run("error-reader", func(t *testing.T) {
		// read nil
		_, err := NewReader(nil).ReadNextCommand(nil)
		assert.NotNil(err)

		for _, prefix := range []byte{BULK, INTEGER, ARRAY} {
			data := append([]byte{prefix}, "an error message"...)
			_, err := NewReader(data).ReadNextCommand(nil)
			assert.NotNil(err)
		}
	})

	t.Run("cutByCRLF", func(t *testing.T) {
		before, after, ok := cutByCRLF([]byte("123\r\n456"))
		assert.Equal(string(before), "123")
		assert.Equal(string(after), "456")
		assert.True(ok)

		before, after, ok = cutByCRLF([]byte("1234\r\n5678"))
		assert.Equal(string(before), "1234")
		assert.Equal(string(after), "5678")
		assert.True(ok)

		// error cases
		_, _, ok = cutByCRLF([]byte("A"))
		assert.False(ok)

		_, _, ok = cutByCRLF([]byte("ABC"))
		assert.False(ok)

		_, _, ok = cutByCRLF([]byte("1234\r"))
		assert.False(ok)
	})

	t.Run("command-bulk", func(t *testing.T) {
		args, err := NewReader([]byte("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")).ReadNextCommand(nil)
		assert.Equal(args[0].ToString(), "SET")
		assert.Equal(args[1].ToString(), "foo")
		assert.Equal(args[2].ToString(), "bar")
		assert.Nil(err)

		// error
		args, err = NewReader([]byte("*A\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")).ReadNextCommand(nil)
		assert.Equal(len(args), 0)
		assert.NotNil(err)

		args, err = NewReader([]byte("*3\r\n$A\r\nGET\r\n$3\r\nfoo\r\n")).ReadNextCommand(nil)
		assert.Equal(len(args), 0)
		assert.NotNil(err)

		args, err = NewReader([]byte("*3\r\n+PING")).ReadNextCommand(nil)
		assert.Equal(len(args), 0)
		assert.NotNil(err)

		args, err = NewReader([]byte("*3\r\n$3ABC")).ReadNextCommand(nil)
		assert.Equal(len(args), 0)
		assert.NotNil(err)
	})

	t.Run("command-inline", func(t *testing.T) {
		args, err := NewReader([]byte("PING\r\n")).ReadNextCommand(nil)
		assert.Equal(args[0].ToString(), "PING")
		assert.Nil(err)
	})
}
