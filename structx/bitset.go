package structx

import (
	"sync"

	"github.com/bits-and-blooms/bitset"
)

// Bitset
type Bitset struct {
	sync.RWMutex
	*bitset.BitSet
}

func NewBitset() *Bitset {
	return &Bitset{
		sync.RWMutex{},
		bitset.New(0),
	}
}

// SetTo
func (bs *Bitset) SetTo(i uint, v bool) *Bitset {
	bs.Lock()
	defer bs.Unlock()
	bs.BitSet.SetTo(i, v)

	return bs
}

// Flip
func (bs *Bitset) Flip(i uint) *Bitset {
	bs.Lock()
	defer bs.Unlock()
	bs.BitSet.Flip(i)

	return bs
}

// Len
func (bs *Bitset) Len() uint {
	bs.RLock()
	defer bs.RUnlock()

	return bs.BitSet.Len()
}

// Union
func (bs *Bitset) Union(bs2 *Bitset) *Bitset {
	bs.Lock()
	defer bs.Unlock()
	bs.BitSet.InPlaceUnion(bs2.BitSet)

	return bs
}

// Intersection
func (bs *Bitset) Intersection(bs2 *Bitset) *Bitset {
	bs.Lock()
	defer bs.Unlock()
	bs.BitSet.InPlaceIntersection(bs2.BitSet)

	return bs
}

// Difference
func (bs *Bitset) Difference(bs2 *Bitset) *Bitset {
	bs.Lock()
	defer bs.Unlock()
	bs.BitSet.InPlaceSymmetricDifference(bs2.BitSet)

	return bs
}

// Clone
func (bs *Bitset) Clone() *Bitset {
	bs.RLock()
	defer bs.RUnlock()

	return &Bitset{
		sync.RWMutex{},
		bs.BitSet.Clone(),
	}
}
