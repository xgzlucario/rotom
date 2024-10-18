package hash

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMap(t *testing.T) {
	testMapI(NewMap(), t)
	testMapI(NewZipMap(), t)
}

func testMapI(m MapI, t *testing.T) {
	ast := assert.New(t)

	// set
	ast.True(m.Set("key1", []byte("val1")))
	ast.True(m.Set("key2", []byte("val2")))
	ast.True(m.Set("key3", []byte("val3")))

	// len
	ast.Equal(m.Len(), 3)

	// get
	val, ok := m.Get("key1")
	ast.True(ok)
	ast.Equal(string(val), "val1")

	val, ok = m.Get("key2")
	ast.True(ok)
	ast.Equal(string(val), "val2")

	val, ok = m.Get("key3")
	ast.True(ok)
	ast.Equal(string(val), "val3")

	val, ok = m.Get("key999")
	ast.False(ok)
	var nilBytes []byte
	ast.Equal(val, nilBytes)

	// set(update great size val)
	ast.False(m.Set("key1", []byte("newval7")))
	ast.False(m.Set("key2", []byte("newval8")))
	ast.False(m.Set("key3", []byte("newval9")))

	// set(update equal size val)
	ast.False(m.Set("key1", []byte("newval1")))
	ast.False(m.Set("key2", []byte("newval2")))
	ast.False(m.Set("key3", []byte("newval3")))

	// get(update)
	val, ok = m.Get("key1")
	ast.True(ok)
	ast.Equal(string(val), "newval1")

	val, ok = m.Get("key2")
	ast.True(ok)
	ast.Equal(string(val), "newval2")

	val, ok = m.Get("key3")
	ast.True(ok)
	ast.Equal(string(val), "newval3")

	val, ok = m.Get("key999")
	ast.False(ok)
	ast.Equal(val, nilBytes)

	// scan
	count := 0
	m.Scan(func(key string, val []byte) {
		switch key {
		case "key1":
			ast.Equal(val, []byte("newval1"))
		case "key2":
			ast.Equal(val, []byte("newval2"))
		case "key3":
			ast.Equal(val, []byte("newval3"))
		}
		count++
	})
	ast.Equal(count, 3)

	// remove
	ast.True(m.Remove("key1"))
	ast.True(m.Remove("key2"))
	ast.True(m.Remove("key3"))
	ast.False(m.Remove("notexist"))

	// scan
	m.Scan(func(string, []byte) {
		panic("should not call")
	})

	// len
	ast.Equal(m.Len(), 0)
}

func TestToMap(t *testing.T) {
	ast := assert.New(t)

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
			ast.Equal(val, []byte("value1"))
		case "key2":
			ast.Equal(val, []byte("value2"))
		case "key3":
			ast.Equal(val, []byte("value3"))
		default:
			panic(fmt.Errorf("error:%s %s", key, val))
		}
		count++
	})
	ast.Equal(count, 3)
}
