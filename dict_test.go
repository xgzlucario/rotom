package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDict(t *testing.T) {
	assert := assert.New(t)

	t.Run("set", func(t *testing.T) {
		dict := New()
		dict.Set("key", []byte("hello"))

		data, ttl := dict.Get("key")
		assert.Equal(ttl, KeepTTL)
		assert.Equal(data, []byte("hello"))

		data, ttl = dict.Get("none")
		assert.Nil(data)
		assert.Equal(ttl, KEY_NOT_EXIST)
	})

	t.Run("setTTL", func(t *testing.T) {
		dict := New()

		dict.SetWithTTL("key", []byte("hello"), time.Now().Add(time.Minute).UnixNano())
		time.Sleep(time.Second / 10)

		data, ttl := dict.Get("key")
		assert.Equal(ttl, 59)
		assert.Equal(data, []byte("hello"))

		res := dict.SetTTL("key", time.Now().Add(-time.Second).UnixNano())
		assert.Equal(res, 1)

		res = dict.SetTTL("not-exist", KeepTTL)
		assert.Equal(res, 0)

		// get expired
		data, ttl = dict.Get("key")
		assert.Equal(ttl, KEY_NOT_EXIST)
		assert.Nil(data)

		// setTTL expired
		dict.SetWithTTL("keyx", []byte("hello"), time.Now().Add(-time.Second).UnixNano())
		res = dict.SetTTL("keyx", 1)
		assert.Equal(res, 0)
	})

	t.Run("delete", func(t *testing.T) {
		dict := New()
		dict.Set("key", []byte("hello"))

		ok := dict.Delete("key")
		assert.True(ok)

		ok = dict.Delete("none")
		assert.False(ok)

		dict.SetWithTTL("keyx", []byte("hello"), time.Now().UnixNano())
		ok = dict.Delete("keyx")
		assert.True(ok)
	})
}
