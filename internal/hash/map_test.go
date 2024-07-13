package hash

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMap(t *testing.T) {
	testMapI(NewMap(), t)
	testMapI(NewZipMap(), t)
}

func testMapI(m MapI, t *testing.T) {
	assert := assert.New(t)

	// set
	assert.True(m.Set("key1", []byte("val1")))
	assert.True(m.Set("key2", []byte("val2")))
	assert.True(m.Set("key3", []byte("val3")))

	// len
	assert.Equal(m.Len(), 3)

	// get
	val, ok := m.Get("key1")
	assert.True(ok)
	assert.Equal(string(val), "val1")

	val, ok = m.Get("key2")
	assert.True(ok)
	assert.Equal(string(val), "val2")

	val, ok = m.Get("key3")
	assert.True(ok)
	assert.Equal(string(val), "val3")

	// set(update)
	assert.False(m.Set("key1", []byte("newval1")))
	assert.False(m.Set("key2", []byte("newval2")))
	assert.False(m.Set("key3", []byte("newval3")))

	// get(update)
	val, ok = m.Get("key1")
	assert.True(ok)
	assert.Equal(string(val), "newval1")

	val, ok = m.Get("key2")
	assert.True(ok)
	assert.Equal(string(val), "newval2")

	val, ok = m.Get("key3")
	assert.True(ok)
	assert.Equal(string(val), "newval3")

	// remove
	assert.True(m.Remove("key1"))
	assert.True(m.Remove("key2"))
	assert.True(m.Remove("key3"))
	assert.False(m.Remove("notexist"))

	// len
	assert.Equal(m.Len(), 0)
}
