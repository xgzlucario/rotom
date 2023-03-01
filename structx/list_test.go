package structx

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// CREATE BY CHATGPT

func TestList(t *testing.T) {
	// Test NewList
	l := NewList(1, 2, 3, 4, 5)

	assert.Equal(t, []int{1, 2, 3, 4, 5}, l.Values())

	// Test LPush
	l.LPush(0)
	assert.Equal(t, []int{0, 1, 2, 3, 4, 5}, l.Values())

	// Test RPush
	l.RPush(6)
	assert.Equal(t, []int{0, 1, 2, 3, 4, 5, 6}, l.Values())

	// Test Insert
	l.Insert(3, 10, 11)
	assert.Equal(t, []int{0, 1, 2, 10, 11, 3, 4, 5, 6}, l.Values())

	// Test RemoveFirst
	assert.True(t, l.RemoveFirst(10))
	assert.Equal(t, []int{0, 1, 2, 11, 3, 4, 5, 6}, l.Values())

	// Test RemoveIndex
	assert.True(t, l.RemoveIndex(3))
	assert.Equal(t, []int{0, 1, 2, 3, 4, 5, 6}, l.Values())

	// Test LPop
	val, ok := l.LPop()
	assert.True(t, ok)
	assert.Equal(t, 0, val)
	assert.Equal(t, []int{1, 2, 3, 4, 5, 6}, l.Values())

	// Test RPop
	val, ok = l.RPop()
	assert.True(t, ok)
	assert.Equal(t, 6, val)
	assert.Equal(t, []int{1, 2, 3, 4, 5}, l.Values())

	// Test AddToSet
	assert.True(t, l.AddToSet(0))
	assert.False(t, l.AddToSet(5))
	assert.Equal(t, []int{1, 2, 3, 4, 5, 0}, l.Values())

	// Test Index
	assert.Equal(t, 0, l.Index(5))

	// Test Find
	assert.Equal(t, 4, l.Find(5))

	// Test Copy
	c := l.Copy()
	assert.Equal(t, l.Values(), c.Values())
	assert.NotEqual(t, fmt.Sprintf("%p", l), fmt.Sprintf("%p", c))

	// Test Len
	assert.Equal(t, 6, l.Len())

	// Test Range
	result := []int{}
	l.Range(func(elem int) bool {
		fmt.Println(elem)
		result = append(result, elem)
		return false
	})
	assert.Equal(t, []int{1, 2, 3, 4, 5, 0}, result)

	// Test LShift
	l.LShift()
	assert.Equal(t, []int{2, 3, 4, 5, 0, 1}, l.Values())

	// Test RShift
	l.RShift()
	assert.Equal(t, []int{1, 2, 3, 4, 5, 0}, l.Values())

	// Test Reverse
	l.Reverse()
	assert.Equal(t, []int{0, 5, 4, 3, 2, 1}, l.Values())
}
