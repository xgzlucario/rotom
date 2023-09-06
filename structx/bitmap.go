package structx

import (
	"sync"

	"github.com/RoaringBitmap/roaring"
)

// Bitmap
type Bitmap struct {
	sync.RWMutex
	bm *roaring.Bitmap
}

func NewBitmap() *Bitmap {
	return &Bitmap{sync.RWMutex{}, roaring.New()}
}

// Add
func (b *Bitmap) Add(i uint32) bool {
	b.Lock()
	defer b.Unlock()
	return b.bm.CheckedAdd(i)
}

// Remove
func (b *Bitmap) Remove(i uint32) bool {
	b.Lock()
	defer b.Unlock()
	return b.bm.CheckedRemove(i)
}

// Test
func (b *Bitmap) Test(i uint32) bool {
	b.RLock()
	defer b.RUnlock()
	return b.bm.Contains(i)
}

// Flip
func (b *Bitmap) Flip(i uint64) {
	b.Lock()
	defer b.Unlock()
	b.bm.Flip(i, i)
}

// ToArray
func (b *Bitmap) ToArray() []uint32 {
	b.Lock()
	defer b.Unlock()
	return b.bm.ToArray()
}

// Len
func (b *Bitmap) Len() uint64 {
	b.RLock()
	defer b.RUnlock()
	return b.bm.Stats().Cardinality
}

// Or
func (b *Bitmap) Or(b2 *Bitmap) *Bitmap {
	b.Lock()
	defer b.Unlock()
	b.bm.Or(b2.bm)
	return b
}

// And
func (b *Bitmap) And(b2 *Bitmap) *Bitmap {
	b.Lock()
	defer b.Unlock()
	b.bm.And(b2.bm)
	return b
}

// Xor
func (b *Bitmap) Xor(b2 *Bitmap) *Bitmap {
	b.Lock()
	defer b.Unlock()
	b.bm.Xor(b2.bm)
	return b
}

// Clone
func (b *Bitmap) Clone() *Bitmap {
	b.RLock()
	defer b.RUnlock()
	return &Bitmap{sync.RWMutex{}, b.bm.Clone()}
}

// MarshalBinary
func (b *Bitmap) MarshalBinary() ([]byte, error) {
	b.RLock()
	defer b.RUnlock()
	return b.bm.MarshalBinary()
}

// UnmarshalBinary
func (b *Bitmap) UnmarshalBinary(data []byte) error {
	b.Lock()
	defer b.Unlock()
	return b.bm.UnmarshalBinary(data)
}
