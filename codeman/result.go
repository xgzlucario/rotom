package codeman

import "encoding/binary"

type Result []byte

func (r Result) ToStr() string {
	return string(r)
}

func (r Result) ToBool() bool {
	return r[0] == _true
}

func (r Result) ToInt64() int64 {
	return int64(parseVarint(r))
}

func (r Result) ToInt() int {
	return int(parseVarint(r))
}

func (r Result) ToUint32() uint32 {
	return uint32(parseVarint(r))
}

func (r Result) ToUint64() uint64 {
	return parseVarint(r)
}

func (r Result) ToStrSlice() []string {
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

func (r Result) ToUint32Slice() []uint32 {
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
