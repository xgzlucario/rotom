package db

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRDB(t *testing.T) {
	ast := assert.New(t)
	path := filepath.Join(t.TempDir(), "dump.rdb")

	d := NewDict()
	d.SetString("str", []byte("v"), 0, false)
	d.SetString("empty", []byte{}, 0, false)
	d.SetString("ttl", []byte("v"), d.Now()+100000, false)
	d.SetString("dead", []byte("v"), d.Now()-1, false)
	h, _ := d.HashForWrite("hash")
	h.Set("f", []byte("v"))
	l, _ := d.ListForWrite("list")
	l.RPush("a")
	small, _ := d.SetForWrite("zipset", nil)
	small.Add("m")
	big, _ := d.SetForWrite("set", make([][]byte, MaxZipSetEntries+1))
	big.Add("m")
	zsmall, _ := d.ZSetForWrite("zipzset", nil)
	zsmall.Set("m", 1.5)
	zbig, _ := d.ZSetForWrite("zset", make([][]byte, MaxZipZSetEntries+1))
	zbig.Set("m", 1.5)
	ast.Nil(SaveRDB(path, d))

	got, err := LoadRDB(path)
	ast.Nil(err)
	ast.Equal(9, got.Len(), "expired keys are not saved")
	at, _ := got.ExpireAt("ttl")
	ast.Equal(d.Now()+100000, at)
	ast.Len(got.volatile, 1)
	want := map[string]Item{}
	for it := range d.All() {
		want[it.Key] = it
	}
	for it := range got.All() {
		w := want[it.Key]
		ast.Equal(EncodingOf(w.Value), EncodingOf(it.Value), it.Key)
		ast.Equal(summary(w.Value), summary(it.Value), it.Key)
		ast.Equal(w.ExpireAt, it.ExpireAt, it.Key)
	}

	t.Run("missing-file", func(t *testing.T) {
		d, err := LoadRDB(filepath.Join(t.TempDir(), "none.rdb"))
		ast.Nil(err)
		ast.Equal(0, d.Len())
	})

	t.Run("corrupted", func(t *testing.T) {
		data, _ := os.ReadFile(path)
		data[len(data)/2] ^= 0xff
		bad := filepath.Join(t.TempDir(), "bad.rdb")
		_ = os.WriteFile(bad, data, 0644)
		_, err := LoadRDB(bad)
		ast.ErrorContains(err, "checksum")

		_ = os.WriteFile(bad, []byte("garbage-garbage-garbage"), 0644)
		_, err = LoadRDB(bad)
		ast.ErrorContains(err, "not a rotom rdb")
	})
}

// summary renders a value's content; per-type encode/decode round trips
// are tested in each data structure package.
func summary(v any) string {
	switch x := v.(type) {
	case []byte:
		return string(x)
	case interface{ Len() int }:
		return fmt.Sprint(x.Len())
	}
	return fmt.Sprintf("%T", v)
}
