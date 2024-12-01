package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDict(t *testing.T) {
	ast := assert.New(t)

	t.Run("set", func(t *testing.T) {
		dict := New()
		dict.Set("key", []byte("hello"))

		data, ttl := dict.Get("key")
		ast.Equal(ttl, KeepTTL)
		ast.Equal(data, []byte("hello"))

		data, ttl = dict.Get("none")
		ast.Nil(data)
		ast.Equal(ttl, KeyNotExist)
	})

	t.Run("setTTL", func(t *testing.T) {
		dict := New()

		dict.SetWithTTL("key", []byte("hello"), time.Now().Add(time.Minute).UnixNano())
		time.Sleep(time.Second / 10)

		data, ttl := dict.Get("key")
		ast.Equal(ttl, int64(59))
		ast.Equal(data, []byte("hello"))

		res := dict.SetTTL("key", time.Now().Add(-time.Second).UnixNano())
		ast.Equal(res, 1)

		res = dict.SetTTL("not-exist", KeepTTL)
		ast.Equal(res, 0)

		// get expired
		data, ttl = dict.Get("key")
		ast.Equal(ttl, KeyNotExist)
		ast.Nil(data)

		// setTTL expired
		dict.SetWithTTL("keyx", []byte("hello"), time.Now().Add(-time.Second).UnixNano())
		res = dict.SetTTL("keyx", 1)
		ast.Equal(res, 0)
	})

	t.Run("delete", func(t *testing.T) {
		dict := New()
		dict.Set("key", []byte("hello"))

		ok := dict.Delete("key")
		ast.True(ok)

		ok = dict.Delete("none")
		ast.False(ok)

		dict.SetWithTTL("keyx", []byte("hello"), time.Now().UnixNano())
		ok = dict.Delete("keyx")
		ast.True(ok)
	})
}
