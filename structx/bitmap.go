package structx

import (
	"math/bits"

	"github.com/bytedance/sonic"
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

// Add return true if num not set yet
func (bm *BitMap) Add(num uint32) bool {
	word, bit := num>>log2BitSize, num%bitSize

	if n := int(word) - len(bm.words); n >= 0 {
		bm.words = append(bm.words, make([]uint64, n+1)...)
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

// Remove return true if num exist
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

// Equal returns whether the two bitmaps are equal
func (bm *BitMap) Equal(t *BitMap) bool {
	return bm.len == t.len && slices.Equal(bm.words, t.words)
}

// Contains check if num exist
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
			// bit and is not 0
			if v&(1<<j) != 0 {
				return bitSize*i + j
			}
		}
	}
	return -1
}

// Union modified current object default.
func (bm *BitMap) Union(t *BitMap) *BitMap {
	bm.len = 0
	bm.resize(len(t.words))

	for i, v := range t.words {
		// OR
		bm.words[i] |= v
		bm.len += bits.OnesCount64(bm.words[i])
	}

	return bm
}

// Intersect modified current object default.
func (bm *BitMap) Intersect(t *BitMap) *BitMap {
	bm.len = 0
	bm.resize(len(t.words))

	for i, v := range t.words {
		// AND
		bm.words[i] &= v
		bm.len += bits.OnesCount64(bm.words[i])
	}

	return bm
}

// Difference modified current object default.
func (bm *BitMap) Difference(t *BitMap) *BitMap {
	bm.len = 0
	bm.resize(len(t.words))

	for i, v := range t.words {
		// NOR
		bm.words[i] ^= v
		bm.len += bits.OnesCount64(bm.words[i])
	}

	return bm
}

// Len
func (bm *BitMap) Len() int {
	return bm.len
}

// resize
func (bm *BitMap) resize(cap int) {
	n := len(bm.words)
	if cap == n {
		return
	}
	if cap < n {
		bm.words = bm.words[:cap]
		return
	}
	bm.words = append(bm.words, make([]uint64, cap-len(bm.words))...)
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

type bitmapJSON struct {
	L int
	W []uint64
}

func (bm *BitMap) MarshalJSON() ([]byte, error) {
	return sonic.Marshal(bitmapJSON{bm.len, bm.words})
}

func (bm *BitMap) UnmarshalJSON(src []byte) error {
	var bmJSON bitmapJSON
	if err := sonic.Unmarshal(src, &bmJSON); err != nil {
		return err
	}

	bm.words = bmJSON.W
	bm.len = bmJSON.L
	return nil
}
