package codec

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRoundTrip(t *testing.T) {
	ast := assert.New(t)
	w := NewWriter(nil)
	w.WriteUint8(7)
	w.WriteUint32(1 << 30)
	w.WriteUint64(math.MaxUint64)
	w.WriteVarint(-42)
	w.WriteUvarint(300)
	w.WriteFloat64(-1.5)
	w.WriteBytes([]byte("bytes"))
	w.WriteString("string")
	w.WriteUvarint(2) // a count: at least that many bytes must follow
	w.WriteUint8(0)
	w.WriteUint8(0)

	r := NewReader(w.Bytes())
	ast.Equal(uint8(7), r.ReadUint8())
	ast.Equal(uint32(1<<30), r.ReadUint32())
	ast.Equal(uint64(math.MaxUint64), r.ReadUint64())
	ast.Equal(int64(-42), r.ReadVarint())
	ast.Equal(uint64(300), r.ReadUvarint())
	ast.Equal(-1.5, r.ReadFloat64())
	b := r.ReadBytes()
	ast.Equal("bytes", string(b))
	ast.Equal("string", r.ReadString())
	ast.Equal(2, r.ReadLen())
	ast.Equal(2, r.Len())

	// ReadBytes returns a copy
	b[0] = 'X'
	ast.Equal("bytes", string(NewReader(w.Bytes()[1+4+8+1+2+8:]).ReadBytes()))
}

func TestCorrupted(t *testing.T) {
	cases := map[string]func(r *Reader){
		"short-fixed": func(r *Reader) { r.ReadUint64() },
		"bad-varint":  func(r *Reader) { r.ReadVarint() },
		"bad-uvarint": func(r *Reader) { r.ReadUvarint() },
		"huge-len":    func(r *Reader) { r.ReadLen() },
		"short-bytes": func(r *Reader) { r.ReadBytes() },
		"empty-uint8": func(r *Reader) { NewReader(nil).ReadUint8() },
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) {
			// 0xff bytes: an unterminated varint, a huge length, too short for fixed sizes
			assert.PanicsWithValue(t, ErrCorrupted, func() { fn(NewReader([]byte{0xff, 0xff})) })
		})
	}
}
