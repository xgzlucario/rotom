package codeman

import (
	"encoding/binary"
	"math"
)

// AnyResult is the bytes result.
type AnyResult []byte

func (r AnyResult) ToStr() string {
	if r == nil {
		return ""
	}
	return string(r)
}

func (r AnyResult) ToStrSlice() []string {
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

func (r AnyResult) ToUint32Slice() []uint32 {
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

func (r VarintResult) ToFloat64() float64 {
	return math.Float64frombits(uint64(r))
}

func (r VarintResult) ToBool() bool {
	return r == 1
}

func (r VarintResult) ToByte() byte {
	return byte(r)
}

func (r VarintResult) ToInt64() int64 {
	return int64(r)
}

func (r VarintResult) ToUint32() uint32 {
	return uint32(r)
}
