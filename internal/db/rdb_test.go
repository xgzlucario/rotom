package db

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xgzlucario/rotom/internal/hash"
	"github.com/xgzlucario/rotom/internal/list"
	"github.com/xgzlucario/rotom/internal/zset"
)

func TestRDB(t *testing.T) {
	ast := assert.New(t)
	path := filepath.Join(t.TempDir(), "dump.rdb")

	d := NewDict()
	d.Set("str", []byte("v"))
	d.SetWithExpire("ttl", []byte("v"), d.Now()+100000)
	d.SetWithExpire("dead", []byte("v"), d.Now()-1)
	h := hash.New()
	h.Set("f", []byte("v"))
	d.Set("hash", h)
	l := list.New()
	l.RPush("a", "b")
	d.Set("list", l)
	zs := hash.NewZipSet()
	zs.Add("m")
	d.Set("zipset", zs)
	d.Set("set", zs.ToSet())
	zz := zset.NewZipZSet()
	zz.Set("m", 1.5)
	d.Set("zipzset", zz)
	d.Set("zset", zz.ToZSet())
	ast.Nil(SaveRDB(path, d))

	got, err := LoadRDB(path)
	ast.Nil(err)
	ast.Equal(8, got.Len(), "expired keys are not saved")
	at, _ := got.ExpireAt("ttl")
	ast.Equal(d.Now()+100000, at)
	for _, key := range []string{"str", "hash", "list", "zipset", "set", "zipzset", "zset"} {
		v1, _ := d.Get(key)
		v2, _ := got.Get(key)
		ast.Equal(EncodingOf(v1), EncodingOf(v2), key)
		ast.Equal(TypeOf(v1), TypeOf(v2), key)
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
