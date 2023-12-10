package codeman

import (
	"encoding/binary"
	"math"
)

// AnyResult is the bytes result.
type AnyResult []byte

func (r AnyResult) Str() string {
	if r == nil {
		return ""
	}
	return string(r)
}

func (r AnyResult) StrSlice() []string {
	if r == nil {
		return nil
	}
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

func (r AnyResult) Uint32Slice() []uint32 {
	if r == nil {
		return nil
	}
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

// VarintResult is the varint result.
type VarintResult uint64

func (r VarintResult) Float64() float64 {
	return math.Float64frombits(uint64(r))
}

func (r VarintResult) Bool() bool {
	return r == 1
}

func (r VarintResult) Byte() byte {
	return byte(r)
}

func (r VarintResult) Int64() int64 {
	return int64(r)
}

func (r VarintResult) Uint32() uint32 {
	return uint32(r)
}
