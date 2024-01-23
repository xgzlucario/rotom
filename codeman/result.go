package codeman

import (
	"encoding/binary"
	"math"
)

// anyResult is the bytes result.
type anyResult []byte

func (r anyResult) Str() string {
	return string(r)
}

func (r anyResult) StrSlice() []string {
	length, n := binary.Uvarint(r)
	r = r[n:]
	data := make([]string, 0, length)
	for i := uint64(0); i < length; i++ {
		klen, n := binary.Uvarint(r)
		r = r[n:]
		data = append(data, string(r[:klen]))
		r = r[klen:]
	}
	return data
}

func (r anyResult) Uint32Slice() []uint32 {
	length, n := binary.Uvarint(r)
	r = r[n:]
	data := make([]uint32, 0, length)
	for i := uint64(0); i < length; i++ {
		k, n := binary.Uvarint(r)
		r = r[n:]
		data = append(data, uint32(k))
	}
	return data
}

// varintResult is the varint result.
type varintResult uint64

func (r varintResult) Float64() float64 {
	return math.Float64frombits(uint64(r))
}

func (r varintResult) Bool() bool {
	return r == 1
}

func (r varintResult) Byte() byte {
	return byte(r)
}

func (r varintResult) Int64() int64 {
	return int64(r)
}

func (r varintResult) Uint32() uint32 {
	return uint32(r)
}
