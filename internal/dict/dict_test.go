package dict

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
)

func genKV(i int) (string, []byte) {
	k := fmt.Sprintf("%09x", i)
	return k, []byte(k)
}

func TestDict(t *testing.T) {
	assert := assert.New(t)
	dict := New(DefaultOptions)

	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("%08x", rand.Int())
		value := fmt.Sprintf("%08x", rand.Int())
		dict.Set(key, []byte(value))

		res, ok := dict.Get(key)
		assert.True(ok)
		assert.Equal(string(res), value)
	}
}
