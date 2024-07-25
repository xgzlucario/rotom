package dict

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDict(t *testing.T) {
	assert := assert.New(t)

	t.Run("set", func(t *testing.T) {
		dict := New()
		dict.Set("key", TypeString, []byte("hello"))

		object, ttl := dict.Get("key")
		assert.Equal(ttl, TTL_DEFAULT)
		assert.Equal(object.Data(), []byte("hello"))
		assert.Equal(object.Type(), TypeString)

		object, ttl = dict.Get("none")
		assert.Nil(object)
		assert.Equal(ttl, KEY_NOT_EXIST)
	})

	t.Run("setTTL", func(t *testing.T) {
		dict := New()
		dict.SetWithTTL("key", TypeString, []byte("hello"), time.Now().Add(time.Minute).UnixNano())

		object, ttl := dict.Get("key")
		assert.Equal(ttl, 60)
		assert.Equal(object.Data(), []byte("hello"))
		assert.Equal(object.Type(), TypeString)

		ttl = dict.SetTTL("key", time.Now().Add(-time.Second).UnixNano())
		assert.Equal(ttl, 1)

		ttl = dict.SetTTL("not-exist", TTL_DEFAULT)
		assert.Equal(ttl, 0)

		// expired
		object, ttl = dict.Get("key")
		assert.Equal(ttl, KEY_NOT_EXIST)
		assert.Nil(object)
	})

	t.Run("delete", func(t *testing.T) {
		dict := New()
		dict.Set("key", TypeString, []byte("hello"))

		ok := dict.Delete("key")
		assert.True(ok)

		ok = dict.Delete("none")
		assert.False(ok)
	})
}
