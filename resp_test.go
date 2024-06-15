package main

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValue(t *testing.T) {
	assert := assert.New(t)

	t.Run("str-value", func(t *testing.T) {
		value := ValueOK
		data := value.Append(nil)
		assert.Equal(string(data), "+OK\r\n")
	})

	t.Run("err-value", func(t *testing.T) {
		value := newErrValue(errors.New("err message"))
		data := value.Append(nil)
		assert.Equal(string(data), "-err message\r\n")
	})

	t.Run("bulk-value", func(t *testing.T) {
		value := newBulkValue([]byte("hello"))
		data := value.Append(nil)
		assert.Equal(string(data), "$5\r\nhello\r\n")

		// empty bulk string
		value = newBulkValue([]byte(""))
		data = value.Append(nil)
		assert.Equal(string(data), "$0\r\n\r\n")

		// nil bulk string
		value = newBulkValue(nil)
		data = value.Append(nil)
		assert.Equal(string(data), "$-1\r\n")
	})

	t.Run("integer-value", func(t *testing.T) {
		value := newIntegerValue(1)
		data := value.Append(nil)
		assert.Equal(string(data), ":1\r\n")
	})

	t.Run("error-value", func(t *testing.T) {
		// read nil
		_, err := NewResp(nil).ReadNextCommand(nil)
		assert.NotNil(err)

		for _, prefix := range []byte{BULK, INTEGER, ARRAY} {
			data := append([]byte{prefix}, "an error message"...)
			_, err := NewResp(data).ReadNextCommand(nil)
			assert.NotNil(err)
		}

		// marshal error type
		value := Value{typ: 76}
		data := value.Append(nil)
		assert.Equal(string(data), ErrUnknownType.Error())
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
	})

	t.Run("readCommandBulk", func(t *testing.T) {
		args, err := NewResp([]byte("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")).ReadNextCommand(nil)
		assert.Equal(args[0].ToString(), "SET")
		assert.Equal(args[1].ToString(), "foo")
		assert.Equal(args[2].ToString(), "bar")
		assert.Nil(err)
	})

	t.Run("readCommandInline", func(t *testing.T) {
		args, err := NewResp([]byte("PING\r\n")).ReadNextCommand(nil)
		assert.Equal(args[0].ToString(), "PING")
		assert.Nil(err)
	})
}
