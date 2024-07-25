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

	val, ok = m.Get("key999")
	assert.False(ok)
	var nilBytes []byte
	assert.Equal(val, nilBytes)

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

	val, ok = m.Get("key999")
	assert.False(ok)
	assert.Equal(val, nilBytes)

	// scan
	count := 0
	m.Scan(func(key string, val []byte) {
		switch key {
		case "key1":
			assert.Equal(val, []byte("newval1"))
		case "key2":
			assert.Equal(val, []byte("newval2"))
		case "key3":
			assert.Equal(val, []byte("newval3"))
		}
		count++
	})
	assert.Equal(count, 3)

	// remove
	assert.True(m.Remove("key1"))
	assert.True(m.Remove("key2"))
	assert.True(m.Remove("key3"))
	assert.False(m.Remove("notexist"))

	// scan
	m.Scan(func(string, []byte) {
		panic("should not call")
	})

	// len
	assert.Equal(m.Len(), 0)
}

func TestToMap(t *testing.T) {
	assert := assert.New(t)

	m := NewZipMap()
	m.Set("key1", []byte("value1"))
	m.Set("key2", []byte("value2"))
	m.Set("key3", []byte("value3"))

	nm := m.ToMap()

	// scan
	count := 0
	nm.Scan(func(key string, val []byte) {
		switch key {
		case "key1":
			assert.Equal(val, []byte("value1"))
		case "key2":
			assert.Equal(val, []byte("value2"))
		case "key3":
			assert.Equal(val, []byte("value3"))
		default:
			panic("error")
		}
		count++
	})
	assert.Equal(count, 3)
}
