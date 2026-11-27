package db

import (
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestDict(t *testing.T) {
	ast := assert.New(t)

	t.Run("set", func(t *testing.T) {
		dict := NewDict()
		dict.Set("key", []byte("hello"))

		data, ok := dict.Get("key")
		ast.True(ok)
		ast.Equal(data, []byte("hello"))
		at, ok := dict.ExpireAt("key")
		ast.True(ok)
		ast.Equal(int64(0), at)

		data, ok = dict.Get("none")
		ast.False(ok)
		ast.Nil(data)
	})

	t.Run("expire", func(t *testing.T) {
		dict := NewDict()
		at := dict.Now() + time.Minute.Milliseconds()
		dict.SetWithExpire("key", []byte("hello"), at)

		got, ok := dict.ExpireAt("key")
		ast.True(ok)
		ast.Equal(at, got)

		// Set keeps the expire time, SetWithExpire replaces it.
		dict.Set("key", []byte("world"))
		got, _ = dict.ExpireAt("key")
		ast.Equal(at, got)
		dict.SetWithExpire("key", []byte("world"), 0)
		got, _ = dict.ExpireAt("key")
		ast.Equal(int64(0), got)
		ast.Empty(dict.volatile)

		// expire time in the past deletes the key
		ast.True(dict.SetExpireAt("key", dict.Now()-1))
		_, ok = dict.Get("key")
		ast.False(ok)
		ast.False(dict.SetExpireAt("not-exist", at))
	})

	t.Run("lazy-expire", func(t *testing.T) {
		dict := NewDict()
		dict.SetWithExpire("key", []byte("hello"), dict.Now()+10)
		time.Sleep(20 * time.Millisecond)
		dict.Tick()
		_, ok := dict.Get("key")
		ast.False(ok)
		ast.Equal(0, dict.Len())
		ast.Empty(dict.volatile)
	})

	t.Run("persist", func(t *testing.T) {
		dict := NewDict()
		dict.SetWithExpire("key", []byte("hello"), dict.Now()+time.Minute.Milliseconds())
		ast.True(dict.Persist("key"))
		ast.False(dict.Persist("key"))
		ast.Empty(dict.volatile)
	})

	t.Run("delete", func(t *testing.T) {
		dict := NewDict()
		dict.Set("key", []byte("hello"))
		ast.True(dict.Delete("key"))
		ast.False(dict.Delete("none"))

		// an expired key counts as missing
		dict.SetWithExpire("keyx", []byte("hello"), dict.Now()-1)
		ast.False(dict.Delete("keyx"))
	})

	t.Run("update-does-not-retain-key", func(t *testing.T) {
		dict := NewDict()
		dict.Set("key", 1)
		// simulate a key that aliases a reused network buffer
		buf := []byte("key")
		alias := unsafe.String(&buf[0], len(buf))
		dict.Set(alias, 2)
		ast.True(dict.SetExpireAt(alias, dict.Now()+1000))
		ast.True(dict.Persist(alias))
		dict.SetWithExpire(alias, 3, dict.Now()+1000)
		copy(buf, "xyz")
		v, ok := dict.Get("key")
		ast.True(ok)
		ast.Equal(3, v)
		_, ok = dict.volatile["key"]
		ast.True(ok)
	})

	t.Run("active-expire", func(t *testing.T) {
		dict := NewDict()
		for i := range 1000 {
			dict.SetWithExpire(fmt.Sprint("dead", i), 1, dict.Now()-1)
		}
		for i := range 100 {
			dict.SetWithExpire(fmt.Sprint("live", i), 1, dict.Now()+time.Hour.Milliseconds())
		}
		for i := range 100 {
			dict.Set(fmt.Sprint("forever", i), 1)
		}
		// expired keys are the majority, so one cycle keeps sampling.
		n := dict.ActiveExpire(time.Second)
		ast.Greater(n, 3*activeExpireSamples)
		// sampling is probabilistic: enough cycles clear every expired key.
		for range 500 {
			dict.ActiveExpire(time.Second)
		}
		ast.Equal(200, dict.Len())
		ast.Len(dict.volatile, 100)
	})
}
