package resp

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func strs(argv [][]byte) []string {
	res := make([]string, len(argv))
	for i, a := range argv {
		res[i] = string(a)
	}
	return res
}

func TestReadCommand(t *testing.T) {
	ast := assert.New(t)

	t.Run("multibulk", func(t *testing.T) {
		in := "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$0\r\n\r\n*1\r\n$4\r\nPING\r\n"
		argv, n, err := ReadCommand([]byte(in), nil)
		ast.Nil(err)
		ast.Equal([]string{"SET", "k", ""}, strs(argv))
		argv, m, err := ReadCommand([]byte(in[n:]), argv)
		ast.Nil(err)
		ast.Equal([]string{"PING"}, strs(argv))
		ast.Equal(len(in), n+m)
	})

	t.Run("binary-safe", func(t *testing.T) {
		argv, _, err := ReadCommand([]byte("*1\r\n$4\r\na\r\nb\r\n"), nil)
		ast.Nil(err)
		ast.Equal([]string{"a\r\nb"}, strs(argv))
	})

	t.Run("incomplete", func(t *testing.T) {
		full := "*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n"
		for i := range len(full) {
			_, _, err := ReadCommand([]byte(full[:i]), nil)
			ast.ErrorIs(err, ErrIncomplete, "prefix %q", full[:i])
		}
	})

	t.Run("inline", func(t *testing.T) {
		argv, n, err := ReadCommand([]byte("set  k \"hello world\" 'x'\r\nrest"), nil)
		ast.Nil(err)
		ast.Equal([]string{"set", "k", "hello world", "x"}, strs(argv))
		ast.Equal(len("set  k \"hello world\" 'x'\r\n"), n)

		argv, _, err = ReadCommand([]byte("\r\n"), nil)
		ast.Nil(err)
		ast.Empty(argv)
	})

	t.Run("protocol-errors", func(t *testing.T) {
		for in, want := range map[string]error{
			"*abc\r\n":                          ErrInvalidMultiBulkLen,
			"*-1\r\n":                           ErrInvalidMultiBulkLen,
			"*99999999\r\n":                     ErrInvalidMultiBulkLen,
			"*1\r\n+x\r\n":                      ErrExpectedBulk,
			"*1\r\n$x\r\n":                      ErrInvalidBulkLen,
			"*1\r\n$1\r\nab\r\n":                ErrInvalidBulkLen,
			"*1\n":                              ErrInvalidMultiBulkLen,
			"get \"k\r\n":                       ErrUnbalancedQuotes,
			strings.Repeat("a", maxInlineLen+1): ErrInlineTooLong,
		} {
			_, _, err := ReadCommand([]byte(in), nil)
			ast.ErrorIs(err, want, "%q", in)
		}
	})
}

func BenchmarkReadCommand(b *testing.B) {
	in := []byte("*3\r\n$3\r\nSET\r\n$16\r\nkey:000000000001\r\n$16\r\nvalue:0000000001\r\n")
	argv := make([][]byte, 0, 8)
	b.ReportAllocs()
	for b.Loop() {
		argv, _, _ = ReadCommand(in, argv)
	}
}
