package dict

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
)

func genKV(i int) (string, []byte) {
	k := fmt.Sprintf("%08x", i)
	return k, []byte(k)
}

func TestDict(t *testing.T) {
	assert := assert.New(t)
	dict := New()

	dict.Set("key1", TypeString, []byte("hello"))
	object, ok := dict.Get("key1")
	assert.True(ok)
	assert.Equal(object.Data(), []byte("hello"))
	assert.Equal(object.Type(), TypeString)
}

func TestDictMultiSet(t *testing.T) {
	assert := assert.New(t)
	dict := New()

	for i := 0; i < 10000; i++ {
		key, value := genKV(rand.Int())
		dict.Set(key, TypeString, value)

		object, ok := dict.Get(key)
		assert.True(ok)
		assert.Equal(object.typ, TypeString)
		assert.Equal(object.data.([]byte), value)
	}
}
