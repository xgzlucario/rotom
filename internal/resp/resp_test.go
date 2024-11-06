package resp

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriter(t *testing.T) {
	ast := assert.New(t)
	writer := NewWriter(16)

	t.Run("string", func(t *testing.T) {
		writer.WriteSString("OK")
		ast.Equal(string(writer.b), "+OK\r\n")
		writer.Reset()
	})

	t.Run("error", func(t *testing.T) {
		writer.WriteError(errors.New("err message"))
		ast.Equal(string(writer.b), "-err message\r\n")
		writer.Reset()
	})

	t.Run("bulk", func(t *testing.T) {
		writer.WriteBulk([]byte("hello"))
		ast.Equal(string(writer.b), "$5\r\nhello\r\n")
		writer.Reset()

		writer.WriteBulkString("world")
		ast.Equal(string(writer.b), "$5\r\nworld\r\n")
		writer.Reset()
	})

	t.Run("integer", func(t *testing.T) {
		writer.WriteInteger(5)
		ast.Equal(string(writer.b), ":5\r\n")
		writer.Reset()
	})

	t.Run("float", func(t *testing.T) {
		writer.WriteFloat(3.1415926)
		ast.Equal(string(writer.b), "$9\r\n3.1415926\r\n")
		writer.Reset()
	})
}

func TestReader(t *testing.T) {
	assert := assert.New(t)

	t.Run("error-reader", func(t *testing.T) {
		// read nil
		_, n, err := NewReader(nil).ReadNextCommand(nil)
		assert.Equal(n, 0)
		assert.NotNil(err)

		for _, prefix := range []byte{BULK, INTEGER, ARRAY} {
			data := append([]byte{prefix}, "an error message"...)
			_, n, err := NewReader(data).ReadNextCommand(nil)
			assert.Equal(n, 0)
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
		cmdStr := []byte("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
		args, n, err := NewReader(cmdStr).ReadNextCommand(nil)
		assert.Equal(args, []RESP{RESP("SET"), RESP("foo"), RESP("bar")})
		assert.Equal(n, len(cmdStr))
		assert.Nil(err)

		// error format cmd
		_, _, err = NewReader([]byte("*A\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")).ReadNextCommand(nil)
		assert.ErrorIs(err, errParseInteger)

		_, _, err = NewReader([]byte("*3\r\n$A\r\nGET\r\n$3\r\nfoo\r\n")).ReadNextCommand(nil)
		assert.ErrorIs(err, errParseInteger)

		_, _, err = NewReader([]byte("*3\r\n+PING")).ReadNextCommand(nil)
		assert.NotNil(err)

		_, _, err = NewReader([]byte("*3\r\n$3ABC")).ReadNextCommand(nil)
		assert.NotNil(err)

		_, _, err = NewReader([]byte("*1\r\n")).ReadNextCommand(nil)
		assert.NotNil(err)

		// multi cmd contains error format
		{
			rd := NewReader([]byte("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n---ERROR MSG---"))
			_, n, err = rd.ReadNextCommand(nil)
			assert.Equal(n, 31)
			assert.Nil(err)

			_, n, err = rd.ReadNextCommand(nil)
			assert.Equal(n, 0)
			assert.NotNil(err)
		}
	})

	t.Run("command-inline", func(t *testing.T) {
		args, n, err := NewReader([]byte("PING\r\n")).ReadNextCommand(nil)
		assert.Equal(args[0], RESP("PING"))
		assert.Equal(n, 6)
		assert.Nil(err)
	})
}

func FuzzRESPReader(f *testing.F) {
	f.Fuzz(func(t *testing.T, b []byte) {
		NewReader(b).ReadNextCommand(nil)
	})
}
