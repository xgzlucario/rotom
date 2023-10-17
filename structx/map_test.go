package structx

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/maps"
)

func TestMap(t *testing.T) {
	assert := assert.New(t)

	m := NewSyncMap[string, int](8)
	valid := map[string]int{}

	for i := 0; i < 10000; i++ {
		m.Set(strconv.Itoa(i), i)
		valid[strconv.Itoa(i)] = i
	}

	assert.Equal(10000, m.Len())

	n, ok := m.Get("50")
	assert.Equal(50, n)
	assert.True(ok)

	ok = m.Delete("20")
	delete(valid, "20")
	assert.True(ok)
	assert.Equal(9999, m.Len())

	n, ok = m.Get("20")
	assert.Equal(0, n)
	assert.False(ok)

	// keys
	assert.ElementsMatch(m.Keys(), maps.Keys(valid))

	// marshal
	data, err := m.MarshalJSON()
	assert.Nil(err)

	m2 := NewSyncMap[string, int]()
	err = m2.UnmarshalJSON(data)
	assert.Nil(err)

	assert.Equal(m.Len(), m2.Len())

	// unmarshal error
	err = m2.UnmarshalJSON([]byte("fake news"))
	assert.NotNil(err)

	// clone
	ml := m.Clone()
	assert.ElementsMatch(ml.Keys(), m.Keys())
}
