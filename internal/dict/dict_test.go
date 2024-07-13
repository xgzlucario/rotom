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

	for i := 0; i < 10000; i++ {
		key, value := genKV(rand.Int())
		dict.Set(key, value)

		res, ok := dict.Get(key)
		assert.True(ok)
		assert.Equal(res.([]byte), value)
	}
}
