package hash

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSet(t *testing.T) {
	testSetI(NewSet(), t)
	testSetI(NewZipSet(), t)
}

func testSetI(m SetI, t *testing.T) {
	assert := assert.New(t)

	// add
	assert.True(m.Add("key1"))
	assert.True(m.Add("key2"))
	assert.True(m.Add("key3"))

	// len
	assert.Equal(m.Len(), 3)

	// scan
	count := 0
	m.Scan(func(key string) {
		switch key {
		case "key1", "key2", "key3":
		default:
			panic("error")
		}
		count++
	})
	assert.Equal(count, 3)

	// remove
	assert.True(m.Remove("key1"))
	assert.True(m.Remove("key2"))
	assert.False(m.Remove("notexist"))

	// pop
	key, ok := m.Pop()
	assert.Equal(key, "key3")
	assert.True(ok)

	key, ok = m.Pop()
	assert.Equal(key, "")
	assert.False(ok)

	// len
	assert.Equal(m.Len(), 0)
}
