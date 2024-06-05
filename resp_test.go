package main

import (
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestValue(t *testing.T) {
	assert := assert.New(t)

	t.Run("str-value", func(t *testing.T) {
		value := ValueOK
		data := value.Marshal()
		assert.Equal(string(data), "+OK\r\n")

		var value2 Value
		err := NewResp(data).Read(&value2)
		assert.NotNil(err)
	})

	t.Run("err-value", func(t *testing.T) {
		value := newErrValue(errors.New("err message"))
		data := value.Marshal()
		assert.Equal(string(data), "-err message\r\n")

		var value2 Value
		err := NewResp(data).Read(&value2)
		assert.NotNil(err)
	})

	t.Run("bulk-value", func(t *testing.T) {
		value := newBulkValue([]byte("hello"))
		data := value.Marshal()
		assert.Equal(string(data), "$5\r\nhello\r\n")
		{
			var value2 Value
			err := NewResp(data).Read(&value2)
			assert.Nil(err)
			assert.Equal(value, value2)
		}

		// empty bulk string
		value = newBulkValue([]byte(""))
		data = value.Marshal()
		assert.Equal(string(data), "$0\r\n\r\n")
		{
			var value2 Value
			err := NewResp(data).Read(&value2)
			assert.Nil(err)
			assert.Equal(value, value2)
		}

		// nil bulk string
		value = newBulkValue(nil)
		data = value.Marshal()
		assert.Equal(string(data), "$-1\r\n")
		{
			var value2 Value
			err := NewResp(data).Read(&value2)
			assert.Nil(err)
			assert.Equal(value, value2)
		}
	})

	t.Run("integer-value", func(t *testing.T) {
		value := newIntegerValue(1)
		data := value.Marshal()
		assert.Equal(string(data), ":1\r\n")

		var value2 Value
		err := NewResp(data).Read(&value2)
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

		var value2 Value
		err := NewResp(data).Read(&value2)
		assert.Nil(err)
		assert.Equal(value, value2)
	})

	t.Run("error-value", func(t *testing.T) {
		// read nil
		var value Value
		err := NewResp(nil).Read(&value)
		assert.NotNil(err)

		for _, prefix := range []byte{BULK, INTEGER, ARRAY} {
			data := append([]byte{prefix}, "an error message"...)
			err := NewResp(data).Read(&value)
			assert.NotNil(err)
		}

		// marshal error type
		value = Value{typ: 76}
		data := value.Marshal()
		assert.Equal(string(data), ErrUnknownType.Error())
	})

	t.Run("to-lower-nocopy", func(t *testing.T) {
		for i := 0; i < 1000; i++ {
			timeFormat := strconv.FormatInt(time.Now().UnixNano(), 36)
			lower := strings.ToLower(timeFormat)
			lower2 := ToLowerNoCopy([]byte(lower))
			assert.Equal(lower, string(lower2))
		}
	})

	t.Run("panic", func(t *testing.T) {
		assert.Panics(func() {
			NewResp([]byte("$2\r\nOK\r\n")).Read(nil)
		})
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
