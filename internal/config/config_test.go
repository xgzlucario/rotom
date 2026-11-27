package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func write(t *testing.T, content string) string {
	path := filepath.Join(t.TempDir(), "rotom.toml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoad(t *testing.T) {
	ast := assert.New(t)

	t.Run("repo-config", func(t *testing.T) {
		c, err := Load("../../rotom.toml")
		ast.Nil(err)
		ast.Equal(6379, c.TCP.Port)
		ast.Equal("everysec", c.AOF.AppendFsync)
	})

	t.Run("defaults", func(t *testing.T) {
		c, err := Load(write(t, "[tcp]\nport = 7000\n"))
		ast.Nil(err)
		ast.Equal(7000, c.TCP.Port)
		ast.Equal("dump.rdb", c.RDB.DBFileName)
		v, ok := c.Get("appendonly")
		ast.True(ok)
		ast.Equal("no", v)
	})

	t.Run("get", func(t *testing.T) {
		c := Default()
		c.AOF.AppendOnly = true
		for name, want := range map[string]string{
			"port": "6379", "appendonly": "yes", "appendfilename": "appendonly.aof",
			"appendfsync": "everysec", "dbfilename": "dump.rdb", "save": "",
			"appenddirname": "appendonlydir", "auto-aof-rewrite-percentage": "100",
			"auto-aof-rewrite-min-size": "67108864",
		} {
			got, ok := c.Get(name)
			ast.True(ok, name)
			ast.Equal(want, got, name)
		}
		_, ok := c.Get("nope")
		ast.False(ok)
	})

	t.Run("size", func(t *testing.T) {
		for in, want := range map[string]int64{"0": 0, "1024": 1024, "64kb": 64 << 10, "64MB": 64 << 20, "1gb": 1 << 30, "5b": 5} {
			n, err := ParseSize(in)
			ast.Nil(err, in)
			ast.Equal(want, n, in)
		}
		for _, in := range []string{"", "x", "-1mb", "1tb", "99999999999gb"} {
			_, err := ParseSize(in)
			ast.NotNil(err, in)
		}
		c, err := Load(write(t, "[aof]\nauto-aof-rewrite-min-size = \"1mb\"\n"))
		ast.Nil(err)
		ast.Equal(Size(1<<20), c.AOF.AutoRewriteMinSize)
	})

	t.Run("missing-file", func(t *testing.T) {
		_, err := Load(filepath.Join(t.TempDir(), "none.toml"))
		ast.NotNil(err)
	})

	t.Run("unknown-key", func(t *testing.T) {
		_, err := Load(write(t, "[aof]\nappendonlyy = true\n"))
		ast.NotNil(err)
	})

	t.Run("invalid", func(t *testing.T) {
		_, err := Load(write(t, "[aof]\nappendfsync = \"sometimes\"\n"))
		ast.NotNil(err)
		_, err = Load(write(t, "[tcp]\nport = 0\n"))
		ast.NotNil(err)
		_, err = Load(write(t, "[rdb]\ndbfilename = \"\"\n"))
		ast.NotNil(err)
		_, err = Load(write(t, "[aof]\nappendfilename = \"\"\n"))
		ast.NotNil(err)
		_, err = Load(write(t, "[aof]\nappendfilename = \"a/b\"\n"))
		ast.NotNil(err)
		_, err = Load(write(t, "[aof]\nappenddirname = \"\"\n"))
		ast.NotNil(err)
		_, err = Load(write(t, "[aof]\nauto-aof-rewrite-percentage = -1\n"))
		ast.NotNil(err)
		_, err = Load(write(t, "[aof]\nauto-aof-rewrite-min-size = \"10xb\"\n"))
		ast.NotNil(err)
	})
}
