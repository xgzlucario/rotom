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
	return &Bitmap{bm: roaring.New()}
}

// Add
func (b *Bitmap) Add(items ...uint32) (n int) {
	b.Lock()
	for _, item := range items {
		if b.bm.CheckedAdd(item) {
			n++
		}
	}
	b.Unlock()
	return
}

// Remove
func (b *Bitmap) Remove(items ...uint32) (n int) {
	b.Lock()
	for _, item := range items {
		if b.bm.CheckedRemove(item) {
			n++
		}
	}
	b.Unlock()
	return
}

// Test
func (b *Bitmap) Test(i uint32) bool {
	b.Lock()
	ok := b.bm.Contains(i)
	b.Unlock()
	return ok
}

// Flip
func (b *Bitmap) Flip(start, end uint64) {
	b.Lock()
	b.bm.Flip(start, end)
	b.Unlock()
}

// ToArray
func (b *Bitmap) ToArray() []uint32 {
	b.Lock()
	arr := b.bm.ToArray()
	b.Unlock()
	return arr
}

// Len
func (b *Bitmap) Len() uint64 {
	b.RLock()
	len := b.bm.Stats().Cardinality
	b.RUnlock()
	return len
}

// Or
func (b *Bitmap) Or(b2 *Bitmap) *Bitmap {
	b.Lock()
	b.bm.Or(b2.bm)
	b.Unlock()
	return b
}

// And
func (b *Bitmap) And(b2 *Bitmap) *Bitmap {
	b.Lock()
	b.bm.And(b2.bm)
	b.Unlock()
	return b
}

// Xor
func (b *Bitmap) Xor(b2 *Bitmap) *Bitmap {
	b.Lock()
	b.bm.Xor(b2.bm)
	b.Unlock()
	return b
}

// Clone
func (b *Bitmap) Clone() *Bitmap {
	b.RLock()
	b2 := &Bitmap{bm: b.bm.Clone()}
	b.RUnlock()
	return b2
}

// MarshalBinary
func (b *Bitmap) MarshalBinary() ([]byte, error) {
	b.RLock()
	src, err := b.bm.MarshalBinary()
	b.RUnlock()
	return src, err
}

// UnmarshalBinary
func (b *Bitmap) UnmarshalBinary(data []byte) error {
	b.Lock()
	err := b.bm.UnmarshalBinary(data)
	b.Unlock()
	return err
}
