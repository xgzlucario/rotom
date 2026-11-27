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

	t.Run("scan-and-all-skip-expired", func(t *testing.T) {
		d := NewDict()
		d.SetString("live", []byte("v"), 0, false)
		d.SetString("dead", []byte("v"), d.Now()-1, false)
		var scanned []string
		for cursor := d.Scan(0, func(k string, _ any) { scanned = append(scanned, k) }); cursor != 0; {
			cursor = d.Scan(cursor, func(k string, _ any) { scanned = append(scanned, k) })
		}
		ast.Equal([]string{"live"}, scanned)
		n := 0
		for range d.All() {
			n++
			break // early stop
		}
		ast.Equal(1, n)
		_, ok := d.ExpireAt("dead")
		ast.False(ok)
		ast.Zero(NewDict().Scan(0, func(string, any) {}))
	})

	t.Run("types", func(t *testing.T) {
		d := New()
		h, _ := d.HashForWrite("h")
		h.Set("f", []byte("v"))
		l, _ := d.ListForWrite("l")
		l.RPush("x")
		s, _ := d.SetForWrite("s", [][]byte{[]byte("m")})
		s.Add("m")
		z, _ := d.ZSetForWrite("z", [][]byte{[]byte("m")})
		z.Set("m", 1)
		d.SetString("str", nil, 0, false)
		for key, want := range map[string]string{"h": "hash", "l": "list", "s": "set", "z": "zset", "str": "string", "none": "none"} {
			ast.Equal(want, d.Type(key).String(), key)
		}
		ast.Equal(TypeNone, TypeOf(42))
		ast.Panics(func() { EncodingOf(42) })
		_, err := newObject(Encoding(99))
		ast.NotNil(err)

		// wrong types and missing keys
		got, err := d.GetList("none")
		ast.Nil(got)
		ast.Nil(err)
		_, err = d.GetSet("h")
		ast.ErrorIs(err, ErrWrongType)
		_, err = d.GetZSet("h")
		ast.ErrorIs(err, ErrWrongType)
		_, err = d.ListForWrite("h")
		ast.ErrorIs(err, ErrWrongType)
		_, err = d.SetForWrite("h", nil)
		ast.ErrorIs(err, ErrWrongType)
		_, err = d.ZSetForWrite("h", nil)
		ast.ErrorIs(err, ErrWrongType)

		d.DeleteIfEmpty("l", 1)
		ast.True(d.Exists("l"))
		d.DeleteIfEmpty("l", 0)
		ast.False(d.Exists("l"))
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
