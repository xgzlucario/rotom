package hash

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSet(t *testing.T) {
	testSetI(NewSet(), t)
	testSetI(NewZipSet(), t)
}

func TestZipSet2Set(t *testing.T) {
	ast := assert.New(t)

	m := NewZipSet()
	m.Add("key1")
	m.Add("key2")
	m.Add("key3")

	ast.ElementsMatch(m.ToSet().ToSlice(), []string{"key1", "key2", "key3"})
}

func testSetI(m SetI, t *testing.T) {
	ast := assert.New(t)

	// add
	ast.True(m.Add("key1"))
	ast.True(m.Add("key2"))
	ast.True(m.Add("key3"))
	ast.False(m.Add("key1"))

	// len
	ast.Equal(m.Len(), 3)

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
	ast.Equal(count, 3)

	// remove
	ast.True(m.Remove("key1"))
	ast.True(m.Remove("key2"))
	ast.False(m.Remove("notexist"))

	// pop
	key, ok := m.Pop()
	ast.Equal(key, "key3")
	ast.True(ok)

	key, ok = m.Pop()
	ast.Equal(key, "")
	ast.False(ok)

	// scan
	m.Scan(func(string) {
		panic("should not call")
	})

	// len
	ast.Equal(m.Len(), 0)
}

func TestToSet(t *testing.T) {
	ast := assert.New(t)

	m := NewZipSet()
	m.Add("key1")
	m.Add("key2")
	m.Add("key3")

	nm := m.ToSet()

	// scan
	count := 0
	nm.Scan(func(key string) {
		switch key {
		case "key1", "key2", "key3":
		default:
			panic("error")
		}
		count++
	})
	ast.Equal(count, 3)
}
