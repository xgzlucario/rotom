package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xgzlucario/rotom/structx"
)

func BenchmarkBitMap(b *testing.B) {
	bm := structx.NewBitMap()
	b.Run("Add", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bm.Add(uint32(i))
		}
	})
	b.Run("Contains", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bm.Contains(uint32(i))
		}
	})
	b.Run("Max", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bm.Max()
		}
	})
	bm1 := bm.Copy()
	b.Run("Or", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bm.Or(bm1)
		}
	})
	b.Run("And", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bm.And(bm1)
		}
	})
	b.Run("Xor", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bm.Xor(bm1)
		}
	})
	b.Run("Remove", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bm.Remove(uint32(i))
		}
	})
}

func TestBitMap(t *testing.T) {
	bm := structx.NewBitMap(1, 3, 5, 7, 9)
	assert.Equal(t, 5, bm.Len())

	bm.Add(1)
	assert.Equal(t, 5, bm.Len())

	bm.Add(2)
	assert.Equal(t, 6, bm.Len())

	assert.True(t, bm.Contains(1))
	assert.False(t, bm.Contains(0))

	assert.Equal(t, 1, bm.Min())
	assert.Equal(t, 9, bm.Max())

	bm.Remove(1)
	assert.Equal(t, 5, bm.Len())
	assert.False(t, bm.Contains(1))

	// Test copy
	bm2 := bm.Copy()
	assert.True(t, bm.Equal(bm2))

	// Test MarshalBinary and UnmarshalBinary
	data, err := bm.MarshalBinary()
	assert.Nil(t, err)

	bm3 := new(structx.BitMap)
	err = bm3.UnmarshalBinary(data)
	assert.Nil(t, err)
	assert.True(t, bm.Equal(bm3))

	// Test Range
	var nums []uint32
	bm.Range(func(num uint32) bool {
		nums = append(nums, num)
		return false
	})
	assert.Equal(t, []uint32{2, 3, 5, 7, 9}, nums)

	// Test RevRange
	nums = nil
	bm.RevRange(func(num uint32) bool {
		nums = append(nums, num)
		return false
	})
	assert.Equal(t, []uint32{9, 7, 5, 3, 2}, nums)

	// Test bitwise operations
	bm = structx.NewBitMap(1, 3, 5, 7, 9)
	bm4 := structx.NewBitMap(1, 2, 3, 4, 5)
	bm5 := structx.NewBitMap(2, 3, 4, 5, 6)
	bm6 := structx.NewBitMap(1, 6)

	// Test Or
	bm7 := bm.Copy().Or(bm4)
	assert.Equal(t, structx.NewBitMap(1, 2, 3, 4, 5, 7, 9), bm7)

	// Test And
	bm8 := bm.Copy().And(bm5)
	assert.Equal(t, structx.NewBitMap(3, 5), bm8)

	// Test Xor
	bm9 := bm.Copy().Xor(bm6)
	assert.Equal(t, structx.NewBitMap(3, 5, 6, 7, 9), bm9)
}
