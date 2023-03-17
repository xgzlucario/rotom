package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xgzlucario/rotom/structx"
)

// CREATE BY CHATGPT

func TestBitMap_Add(t *testing.T) {
	bm := structx.NewBitMap()

	// Add new numbers
	assert.True(t, bm.Add(10))
	assert.True(t, bm.Add(20))
	assert.True(t, bm.Add(30))

	// Add existing number
	assert.False(t, bm.Add(10))

	// Check the length and contents
	assert.Equal(t, 3, bm.Len())
	assert.True(t, bm.Contains(10))
	assert.True(t, bm.Contains(20))
	assert.True(t, bm.Contains(30))
}

func TestBitMap_AddRange(t *testing.T) {
	bm := structx.NewBitMap()

	// Add range of numbers
	bm.AddRange(10, 20)

	// Check the length and contents
	assert.Equal(t, 10, bm.Len())
	for i := 10; i < 20; i++ {
		assert.True(t, bm.Contains(uint32(i)))
	}
}

func TestBitMap_Remove(t *testing.T) {
	bm := structx.NewBitMap(10, 20, 30)

	// Remove existing numbers
	assert.True(t, bm.Remove(10))
	assert.True(t, bm.Remove(20))

	// Remove non-existing number
	assert.False(t, bm.Remove(40))

	// Check the length and contents
	assert.Equal(t, 1, bm.Len())
	assert.True(t, bm.Contains(30))
}

func TestBitMap_Equal(t *testing.T) {
	bm1 := structx.NewBitMap(10, 20, 30)
	bm2 := structx.NewBitMap(10, 30, 20)

	if !bm1.Equal(bm2) {
		t.Errorf("Expected bitmaps to be equal, but they were not")
	}
}

func TestBitMap_Min(t *testing.T) {
	bm := structx.NewBitMap(10, 20, 30)

	// Check minimum value
	assert.Equal(t, 10, bm.Min())
}

func TestBitMap_Max(t *testing.T) {
	bm := structx.NewBitMap(10, 20, 30)

	// Check maximum value
	assert.Equal(t, 30, bm.Max())
}

func TestBitMap_Union(t *testing.T) {
	bm1 := structx.NewBitMap(10, 20, 30)
	bm2 := structx.NewBitMap(20, 30, 40)

	// Union
	bm1.Union(bm2)

	// Check the length and contents
	assert.Equal(t, 4, bm1.Len())
	assert.True(t, bm1.Contains(10))
	assert.True(t, bm1.Contains(20))
}
