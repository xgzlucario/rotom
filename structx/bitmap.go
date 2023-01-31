package structx

import (
	"github.com/xgzlucario/rotom/base"
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

// NewBitMap return bitmap object.
func NewBitMap(nums ...uint32) *BitMap {
	bm := new(BitMap)
	for _, num := range nums {
		bm.Add(num)
	}
	return bm
}

// Add
// If you use numbers that increment from 0, the bitmap performance will be very good.
func (bm *BitMap) Add(num uint32) bool {
	word, bit := num>>log2BitSize, num%bitSize

	if n := int(word) - len(bm.words); n >= 0 {
		bm.words = append(bm.words, make([]uint64, n+1)...)
	}

	// bit and is 0
	if bm.words[word]&(1<<bit) == 0 {
		// SET 1
		bm.words[word] |= 1 << bit
		bm.len++
		return true
	}
	return false
}

// AddRange
func (bm *BitMap) AddRange(start uint32, end uint32) *BitMap {
	for i := start; i < end; i++ {
		bm.Add(i)
	}
	return bm
}

// Remove
func (bm *BitMap) Remove(num uint32) bool {
	word, bit := num>>log2BitSize, num%bitSize
	if int(word) >= len(bm.words) {
		return false
	}

	// // bit and is not 0
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
	return slices.Equal(bm.words, t.words)
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
		for j := 0; j < bitSize; j++ {
			// bit and is not 0
			if v&(1<<j) != 0 {
				return bitSize*i + j
			}
		}
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

// Union
func (bm *BitMap) Union(t *BitMap, inplace ...bool) *BitMap {
	// modify inplace
	if len(inplace) > 0 && inplace[0] {
		// append
		if n := len(t.words) - len(bm.words); n >= 0 {
			bm.words = append(bm.words, make([]uint64, n+1)...)
		}

		for i, v := range t.words {
			bm.words[i] |= v
		}
		return nil

	} else {
		min, max := bm.compareLength(t)
		// copy max object
		max = max.Copy()

		for i, v := range min.words {
			max.words[i] |= v
		}

		return max
	}
}

// Intersect
func (bm *BitMap) Intersect(target *BitMap, inplace ...bool) *BitMap {
	// modify inplace
	if len(inplace) > 0 && inplace[0] {
		if len(bm.words) < len(target.words) {
			for i := range bm.words {
				// AND
				bm.words[i] &= target.words[i]
			}

		} else {
			for i, v := range target.words {
				// AND
				bm.words[i] &= v
			}
			for i := len(target.words); i < len(bm.words); i++ {
				// SET 0
				bm.words[i] &= 0
			}
		}
		return nil

	} else {
		min, max := bm.compareLength(target)
		// copy min object
		min = min.Copy()

		for i, v := range max.words {
			if i >= len(min.words) {
				break
			}
			// AND
			min.words[i] &= v
		}
		return min
	}
}

// Difference
func (bm *BitMap) Difference(t *BitMap, inplace ...bool) *BitMap {
	// modify inplace
	if len(inplace) > 0 && inplace[0] {
		// append
		if n := len(t.words) - len(bm.words); n >= 0 {
			bm.words = append(bm.words, make([]uint64, n+1)...)
		}

		for i, v := range t.words {
			// NOR
			bm.words[i] ^= v
		}
		return nil

	} else {
		min, max := bm.compareLength(t)
		// copy max object
		max = max.Copy()

		for i := range max.words {
			if i >= len(min.words) {
				break
			}
			// NOR
			max.words[i] ^= min.words[i]
		}
		return max
	}
}

// Len
func (bm *BitMap) Len() int {
	return bm.len
}

// Copy
func (bm *BitMap) Copy() *BitMap {
	return &BitMap{
		words: slices.Clone(bm.words),
		len:   bm.len,
	}
}

// Range: Not recommended for poor performance
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

// RevRange: Not recommended for poor performance
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

// Compare two bitmap length and return (*min, *max)
func (bm1 *BitMap) compareLength(bm2 *BitMap) (*BitMap, *BitMap) {
	if len(bm1.words) < len(bm2.words) {
		return bm1, bm2
	}
	return bm2, bm1
}

// marshal type
type bitmapJSON struct {
	L int
	W []uint64
}

func (bm *BitMap) MarshalJSON() ([]byte, error) {
	return base.MarshalJSON(bitmapJSON{bm.len, bm.words})
}

func (bm *BitMap) UnmarshalJSON(src []byte) error {
	var bmJSON bitmapJSON
	if err := base.UnmarshalJSON(src, &bmJSON); err != nil {
		return err
	}

	bm.words = bmJSON.W
	bm.len = bmJSON.L
	return nil
}
