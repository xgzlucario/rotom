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
		assert.Equal(string(writer.b), "+OK\r\n")
		writer.Reset()
	})

	t.Run("error", func(t *testing.T) {
		writer.WriteError(errors.New("err message"))
		assert.Equal(string(writer.b), "-err message\r\n")
		writer.Reset()
	})

	t.Run("bulk", func(t *testing.T) {
		writer.WriteBulk([]byte("hello"))
		assert.Equal(string(writer.b), "$5\r\nhello\r\n")
		writer.Reset()

		writer.WriteBulkString("world")
		assert.Equal(string(writer.b), "$5\r\nworld\r\n")
		writer.Reset()
	})

	t.Run("integer", func(t *testing.T) {
		writer.WriteInteger(5)
		assert.Equal(string(writer.b), ":5\r\n")
		writer.Reset()
	})

	t.Run("float", func(t *testing.T) {
		writer.WriteFloat(3.1415926)
		assert.Equal(string(writer.b), "$9\r\n3.1415926\r\n")
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

	t.Run("parseInt", func(t *testing.T) {
		n, after, err := parseInt([]byte("3\r\nHELLO"))
		assert.Equal(n, 3)
		assert.Equal(after, []byte("HELLO"))
		assert.Nil(err)

		n, after, err = parseInt([]byte("003\r\nHELLO"))
		assert.Equal(n, 3)
		assert.Equal(after, []byte("HELLO"))
		assert.Nil(err)

		// errors
		_, _, err = parseInt([]byte("ABC\r\nHELLO"))
		assert.ErrorIs(err, errParseInteger)

		_, _, err = parseInt([]byte("1234567\r"))
		assert.ErrorIs(err, errCRLFNotFound)

		_, _, err = parseInt([]byte("1234567"))
		assert.ErrorIs(err, errCRLFNotFound)
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
		assert.ErrorIs(err, errParseInteger)

		args, err = NewReader([]byte("*3\r\n$A\r\nGET\r\n$3\r\nfoo\r\n")).ReadNextCommand(nil)
		assert.Equal(len(args), 0)
		assert.ErrorIs(err, errParseInteger)

		args, err = NewReader([]byte("*3\r\n+PING")).ReadNextCommand(nil)
		assert.Equal(len(args), 0)
		assert.NotNil(err)

		args, err = NewReader([]byte("*3\r\n$3ABC")).ReadNextCommand(nil)
		assert.Equal(len(args), 0)
		assert.NotNil(err)

		args, err = NewReader([]byte("*1\r\n")).ReadNextCommand(nil)
		assert.Equal(len(args), 0)
		assert.NotNil(err)
	})

	t.Run("command-inline", func(t *testing.T) {
		args, err := NewReader([]byte("PING\r\n")).ReadNextCommand(nil)
		assert.Equal(args[0].ToString(), "PING")
		assert.Nil(err)
	})
}

func FuzzRESPReader(f *testing.F) {
	f.Fuzz(func(t *testing.T, b []byte) {
		NewReader(b).ReadNextCommand(nil)
	})
}
