package structx

import (
	"encoding/binary"
	"errors"
	"math/bits"

	"golang.org/x/exp/slices"
)

const (
	bitSize     = 64 // uint64 is 64 bits
	log2BitSize = 6
)

type BitMap struct {
	len   int
	words []uint64
}

// NewBitMap
func NewBitMap(nums ...uint32) *BitMap {
	bm := new(BitMap)
	for _, num := range nums {
		bm.Add(num)
	}
	return bm
}

// Add
func (bm *BitMap) Add(num uint32) bool {
	word, bit := num>>log2BitSize, num%bitSize

	for len(bm.words) <= int(word) {
		bm.words = append(bm.words, uint64(0))
	}

	// not exist
	if bm.words[word]&(1<<bit) == 0 {
		// SET 1
		bm.words[word] |= 1 << bit
		bm.len++
		return true
	}
	return false
}

// Remove
func (bm *BitMap) Remove(num uint32) bool {
	word, bit := num>>log2BitSize, num%bitSize
	if int(word) >= len(bm.words) {
		return false
	}

	// exist
	if bm.words[word]&(1<<bit) != 0 {
		// SET 0
		bm.words[word] &^= 1 << bit
		bm.len--
		return true
	}
	return false
}

// Equal
func (bm *BitMap) Equal(t *BitMap) bool {
	return bm.len == t.len && slices.Equal(bm.words, t.words)
}

// Contains
func (bm *BitMap) Contains(num uint32) bool {
	word, bit := num/bitSize, num%bitSize
	return int(word) < len(bm.words) && bm.words[word]&(1<<bit) != 0
}

// Min
func (bm *BitMap) Min() int {
	for i, v := range bm.words {
		if v == 0 {
			continue
		}
		return bitSize*i + bits.TrailingZeros64(v)
	}
	return -1
}

// Max
func (bm *BitMap) Max() int {
	for i := len(bm.words) - 1; i >= 0; i-- {
		v := bm.words[i]
		if v == 0 {
			continue
		}
		for j := bitSize - 1; j >= 0; j-- {
			if v&(1<<j) != 0 {
				return bitSize*i + j
			}
		}
	}
	return -1
}

// Or
func (bm *BitMap) Or(t *BitMap) *BitMap {
	bm.len = 0
	for t.Cap() > bm.Cap() {
		bm.words = append(bm.words, uint64(0))
	}

	for i, v := range t.words {
		bm.words[i] |= v
		bm.len += bits.OnesCount64(bm.words[i])
	}

	return bm
}

// And
func (bm *BitMap) And(t *BitMap) *BitMap {
	bm.len = 0

	for i, v := range t.words {
		if i >= bm.Cap() {
			break
		}
		bm.words[i] &= v
		bm.len += bits.OnesCount64(bm.words[i])
	}

	return bm
}

// Xor
func (bm *BitMap) Xor(t *BitMap) *BitMap {
	bm.len = 0
	for t.Cap() > bm.Cap() {
		bm.words = append(bm.words, uint64(0))
	}

	for i, v := range t.words {
		bm.words[i] ^= v
		bm.len += bits.OnesCount64(bm.words[i])
	}

	return bm
}

// Len
func (bm *BitMap) Len() int {
	return bm.len
}

// Cap
func (bm *BitMap) Cap() int {
	return cap(bm.words)
}

// Copy
func (bm *BitMap) Copy() *BitMap {
	return &BitMap{bm.len, slices.Clone(bm.words)}
}

// Range
func (bm *BitMap) Range(f func(uint32) bool) {
	for i, v := range bm.words {
		if v == 0 {
			continue
		}
		for j := uint32(0); j < bitSize; j++ {
			// bit and is not 0
			if v&(1<<j) != 0 {
				if f(bitSize*uint32(i) + j) {
					return
				}
			}
		}
	}
}

// RevRange
func (bm *BitMap) RevRange(f func(uint32) bool) {
	for i := len(bm.words) - 1; i >= 0; i-- {
		v := bm.words[i]
		if v == 0 {
			continue
		}
		for j := bitSize - 1; j >= 0; j-- {
			// bit and is not 0
			if v&(1<<j) != 0 {
				if f(bitSize*uint32(i) + uint32(j)) {
					return
				}
			}
		}
	}
}

// MarshalBinary
func (bm *BitMap) MarshalBinary() ([]byte, error) {
	buf := make([]byte, 0, len(bm.words)*bitSize)

	buf = binary.BigEndian.AppendUint64(buf, uint64(bm.len))
	for _, v := range bm.words {
		buf = binary.BigEndian.AppendUint64(buf, v)
	}

	return buf, nil
}

// UnmarshalBinary
func (bm *BitMap) UnmarshalBinary(src []byte) error {
	if len(src) < 8 {
		return errors.New("unmarshal error")
	}

	bm.len = int(binary.BigEndian.Uint64(src[0:8]))
	bm.words = make([]uint64, 0, len(src)/8-1)

	for i := 8; i < len(src); i += 8 {
		bm.words = append(bm.words, binary.BigEndian.Uint64(src[i:i+8]))
	}

	return nil
}
