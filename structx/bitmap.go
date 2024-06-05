package structx

import (
	"github.com/RoaringBitmap/roaring"
)

type Bitmap struct {
	bm *roaring.Bitmap
}

func NewBitmap() *Bitmap {
	return &Bitmap{bm: roaring.New()}
}

func (b *Bitmap) Add(items ...uint32) (n int) {
	for _, item := range items {
		if b.bm.CheckedAdd(item) {
			n++
		}
	}
	return
}

func (b *Bitmap) Remove(items ...uint32) (n int) {
	for _, item := range items {
		if b.bm.CheckedRemove(item) {
			n++
		}
	}
	return
}

func (b *Bitmap) Test(i uint32) bool {
	return b.bm.Contains(i)
}

func (b *Bitmap) Flip(start, end uint64) {
	b.bm.Flip(start, end)
}

func (b *Bitmap) ToArray() []uint32 {
	return b.bm.ToArray()
}

func (b *Bitmap) Len() uint64 {
	return b.bm.Stats().Cardinality
}

func (b *Bitmap) Or(b2 *Bitmap) *Bitmap {
	b.bm.Or(b2.bm)
	return b
}

func (b *Bitmap) And(b2 *Bitmap) *Bitmap {
	b.bm.And(b2.bm)
	return b
}

func (b *Bitmap) Xor(b2 *Bitmap) *Bitmap {
	b.bm.Xor(b2.bm)
	return b
}

func (b *Bitmap) Clone() *Bitmap {
	return &Bitmap{bm: b.bm.Clone()}
}

func (b *Bitmap) MarshalBinary() ([]byte, error) {
	return b.bm.MarshalBinary()
}

func (b *Bitmap) UnmarshalBinary(data []byte) error {
	return b.bm.UnmarshalBinary(data)
}
