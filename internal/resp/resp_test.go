package resp

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriter(t *testing.T) {
	ast := assert.New(t)
	w := NewWriter(16)
	w.WriteString("OK\r\nX")
	w.WriteError("ERR x")
	w.WriteInt(-42)
	w.WriteBulkString("ab")
	w.WriteBulk(nil)
	w.WriteNull()
	w.WriteArray(2)
	w.WriteFloat(1000000)
	w.WriteFloat(math.Inf(-1))
	ast.Equal("+OK  X\r\n-ERR x\r\n:-42\r\n$2\r\nab\r\n$0\r\n\r\n$-1\r\n*2\r\n$7\r\n1000000\r\n$4\r\n-inf\r\n", string(w.Bytes()))

	mark := w.Len()
	w.WriteString("discard")
	w.Truncate(mark)
	ast.Equal(mark, w.Len())

	w.Reset()
	ast.Equal(0, w.Len())
	ast.True(IsError([]byte("-ERR")))
	ast.False(IsError(nil))
}

func TestAppendFloat(t *testing.T) {
	cases := map[float64]string{
		0: "0", 300.5: "300.5", -1.5: "-1.5", 1e21: "1e+21", 0.1: "0.1", math.Inf(1): "inf",
	}
	for f, want := range cases {
		assert.Equal(t, want, string(AppendFloat(nil, f)))
	}
}
