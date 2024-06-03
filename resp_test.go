package main

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValue(t *testing.T) {
	assert := assert.New(t)

	t.Run("str-value", func(t *testing.T) {
		value := ValueOK
		data := value.Marshal()
		assert.Equal(string(data), "+OK\r\n")

		_, err := NewResp(bytes.NewReader(data)).Read()
		assert.NotNil(err)
	})

	t.Run("err-value", func(t *testing.T) {
		value := newErrValue(errors.New("err message"))
		data := value.Marshal()
		assert.Equal(string(data), "-err message\r\n")

		_, err := NewResp(bytes.NewReader(data)).Read()
		assert.NotNil(err)
	})

	t.Run("bulk-value", func(t *testing.T) {
		value := newBulkValue([]byte("hello"))
		data := value.Marshal()
		assert.Equal(string(data), "$5\r\nhello\r\n")

		value2, err := NewResp(bytes.NewReader(data)).Read()
		assert.Nil(err)
		assert.Equal(value, value2)

		// empty bulk string
		value = newBulkValue([]byte(""))
		data = value.Marshal()
		assert.Equal(string(data), "$0\r\n\r\n")

		value2, err = NewResp(bytes.NewReader(data)).Read()
		assert.Nil(err)
		assert.Equal(value, value2)

		// nil bulk string
		value = newBulkValue(nil)
		data = value.Marshal()
		assert.Equal(string(data), "$-1\r\n")

		value2, err = NewResp(bytes.NewReader(data)).Read()
		assert.Nil(err)
		assert.Equal(value, value2)
	})

	t.Run("integer-value", func(t *testing.T) {
		value := newIntegerValue(1)
		data := value.Marshal()
		assert.Equal(string(data), ":1\r\n")

		value2, err := NewResp(bytes.NewReader(data)).Read()
		assert.Nil(err)
		assert.Equal(value, value2)
	})

	t.Run("array-value", func(t *testing.T) {
		value := newArrayValue([]Value{
			{typ: INTEGER, num: 1},
			{typ: INTEGER, num: 2},
			{typ: INTEGER, num: 3},
			{typ: BULK, bulk: []byte("hello")},
			{typ: BULK, bulk: []byte("world")},
		})
		data := value.Marshal()
		assert.Equal(string(data), "*5\r\n:1\r\n:2\r\n:3\r\n$5\r\nhello\r\n$5\r\nworld\r\n")

		value2, err := NewResp(bytes.NewReader(data)).Read()
		assert.Nil(err)
		assert.Equal(value, value2)
	})

	t.Run("error-value", func(t *testing.T) {
		// read nil
		_, err := NewResp(bytes.NewReader(nil)).Read()
		assert.NotNil(err)

		for _, prefix := range []byte{BULK, INTEGER, ARRAY} {
			data := append([]byte{prefix}, "an error message"...)
			_, err := NewResp(bytes.NewReader(data)).Read()
			assert.NotNil(err)
		}

		// marshal error type
		value := Value{typ: 76}
		data := value.Marshal()
		assert.Equal(string(data), ErrUnknownType.Error())
	})
}

func BenchmarkRESP(b *testing.B) {
	b.Run("str-value", func(b *testing.B) {
		value := ValueOK
		for i := 0; i < b.N; i++ {
			value.Marshal()
		}
	})
	b.Run("bulk-value", func(b *testing.B) {
		value := newBulkValue([]byte("hello"))
		for i := 0; i < b.N; i++ {
			value.Marshal()
		}
	})
	b.Run("integer-value", func(b *testing.B) {
		value := newIntegerValue(100)
		for i := 0; i < b.N; i++ {
			value.Marshal()
		}
	})
}
