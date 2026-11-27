package db

import (
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func str(d *Dict, key string) string {
	v, _, _ := d.GetString(key)
	return string(v)
}

func TestDict(t *testing.T) {
	ast := assert.New(t)

	t.Run("string", func(t *testing.T) {
		d := NewDict()
		d.SetString("key", []byte("hello"), 0, false)
		v, ok, err := d.GetString("key")
		ast.True(ok)
		ast.Nil(err)
		ast.Equal("hello", string(v))
		ast.Equal(TypeString, d.Type("key"))

		_, ok, _ = d.GetString("none")
		ast.False(ok)
		ast.Equal(TypeNone, d.Type("none"))

		// containers are not strings
		_, _ = d.ListForWrite("list")
		_, _, err = d.GetString("list")
		ast.ErrorIs(err, ErrWrongType)
		_, err = d.GetHash("key")
		ast.ErrorIs(err, ErrWrongType)

		// overwriting a container with a string
		d.SetString("list", []byte("x"), 0, false)
		ast.Equal(TypeString, d.Type("list"))
	})

	t.Run("overwrite-reuses-buffer", func(t *testing.T) {
		d := NewDict()
		d.SetString("k", []byte("aaaa"), 0, false)
		v1, _, _ := d.GetString("k")
		d.SetString("k", []byte("bbbb"), 0, false)
		v2, _, _ := d.GetString("k")
		ast.Equal(unsafe.SliceData(v1), unsafe.SliceData(v2))
		ast.Equal("bbbb", string(v2))
		ast.Equal(0.0, testing.AllocsPerRun(100, func() {
			d.SetString("k", []byte("cccc"), 0, false)
		}))
	})

	t.Run("expire", func(t *testing.T) {
		d := NewDict()
		at := d.Now() + time.Minute.Milliseconds()
		d.SetString("key", []byte("v"), at, false)
		got, _ := d.ExpireAt("key")
		ast.Equal(at, got)

		d.SetString("key", []byte("v2"), 0, true) // KEEPTTL
		got, _ = d.ExpireAt("key")
		ast.Equal(at, got)
		d.SetString("key", []byte("v3"), 0, false)
		got, _ = d.ExpireAt("key")
		ast.Zero(got)
		ast.Empty(d.volatile)

		ast.True(d.SetExpireAt("key", at))
		ast.Len(d.volatile, 1)
		ast.True(d.Persist("key"))
		ast.False(d.Persist("key"))
		ast.Empty(d.volatile)

		ast.True(d.SetExpireAt("key", d.Now()-1)) // in the past: deleted
		ast.False(d.Exists("key"))
		ast.False(d.SetExpireAt("none", at))
	})

	t.Run("lazy-expire", func(t *testing.T) {
		d := NewDict()
		d.SetString("key", []byte("v"), d.Now()+10, false)
		time.Sleep(20 * time.Millisecond)
		d.Tick()
		ast.False(d.Exists("key"))
		ast.Equal(0, d.Len())
		ast.Empty(d.volatile)
		ast.False(d.Delete("key"))
	})

	t.Run("keys-are-copied", func(t *testing.T) {
		d := NewDict()
		buf := []byte("key")
		d.SetString(unsafe.String(&buf[0], len(buf)), []byte("v"), d.Now()+1000, false)
		copy(buf, "xyz")
		ast.True(d.Exists("key"))
	})

	t.Run("active-expire", func(t *testing.T) {
		d := NewDict()
		for i := range 1000 {
			d.SetString(fmt.Sprint("dead", i), nil, d.Now()-1, false)
		}
		for i := range 100 {
			d.SetString(fmt.Sprint("live", i), nil, d.Now()+time.Hour.Milliseconds(), false)
			d.SetString(fmt.Sprint("forever", i), nil, 0, false)
		}
		// expired keys are the majority, so one cycle keeps sampling.
		ast.Greater(d.ActiveExpire(time.Second), 3*activeExpireSamples)
		// sampling is probabilistic: enough cycles clear every expired key.
		for range 500 {
			d.ActiveExpire(time.Second)
		}
		ast.Equal(200, d.Len())
		ast.Len(d.volatile, 100)
	})
}
