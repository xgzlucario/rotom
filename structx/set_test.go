package structx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSet(t *testing.T) {
	assert := assert.New(t)
	s := NewSet[int]()

	for i := 0; i < 10; i++ {
		s.Add(i)
	}
	assert.Equal(s.Len(), 10)

	// clone
	assert.ElementsMatch(s.ToSlice(), []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	assert.ElementsMatch(s.Clone().ToSlice(), []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

	for i := 0; i < 10; i++ {
		assert.True(s.Has(i))
	}
	for i := 10; i < 20; i++ {
		assert.False(s.Has(i))
	}

	for i := 0; i < 10; i++ {
		assert.True(s.Remove(i))
	}
	for i := 10; i < 20; i++ {
		assert.False(s.Remove(i))
	}
	assert.Equal(s.Len(), 0)
}

func TestSetUnion(t *testing.T) {
	assert := assert.New(t)

	s1 := NewSet[int](1, 3, 5)
	s2 := NewSet[int](2, 4, 5)
	s1.Union(s2)

	assert.Equal(s1.Len(), 5)
	assert.ElementsMatch(s1.ToSlice(), []int{1, 2, 3, 4, 5})
}

func TestSetIntersect(t *testing.T) {
	assert := assert.New(t)

	s1 := NewSet[int](1, 3, 4, 5)
	s2 := NewSet[int](2, 4, 5)
	s1.Intersect(s2)

	assert.Equal(s1.Len(), 2)
	assert.ElementsMatch(s1.ToSlice(), []int{4, 5})
}

func TestSetDifference(t *testing.T) {
	assert := assert.New(t)

	s1 := NewSet[int](1, 3, 4, 5)
	s2 := NewSet[int](2, 4, 5)
	s1.Difference(s2)

	assert.Equal(s1.Len(), 3)
	assert.ElementsMatch(s1.ToSlice(), []int{1, 2, 3})
}

func TestSetMarshal(t *testing.T) {
	assert := assert.New(t)
	nums := []int{1, 3, 5, 7, 9, 11, 13, 15, 17, 19}

	s := NewSet[int](nums...)
	// marshal
	src, err := s.MarshalJSON()
	assert.Nil(err)
	assert.Equal(string(src), "[1,3,5,7,9,11,13,15,17,19]")

	var dst Set[int]
	// unmarshal
	err = dst.UnmarshalJSON(src)
	assert.Nil(err)
	assert.ElementsMatch(dst.ToSlice(), nums)

	// unmarshal error
	err = dst.UnmarshalJSON([]byte("error"))
	assert.NotNil(err)
}
