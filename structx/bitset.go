package structx

import (
	"github.com/bits-and-blooms/bitset"
)

// Bitset
type Bitset struct {
	*bitset.BitSet
}

func NewBitset() *Bitset {
	return &Bitset{bitset.New(0)}
}

// Union
func (bs1 *Bitset) Union(bs2 *Bitset) *Bitset {
	bs1.BitSet.InPlaceUnion(bs2.BitSet)
	return bs1
}

// Intersection
func (bs1 *Bitset) Intersection(bs2 *Bitset) *Bitset {
	bs1.BitSet.InPlaceIntersection(bs2.BitSet)
	return bs1
}

// Difference
func (bs1 *Bitset) Difference(bs2 *Bitset) *Bitset {
	bs1.BitSet.InPlaceSymmetricDifference(bs2.BitSet)
	return bs1
}

// Clone
func (bs *Bitset) Clone() *Bitset {
	return &Bitset{bs.BitSet.Clone()}
}
