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

	t.Run("unknown-key", func(t *testing.T) {
		_, err := Load(write(t, "[aof]\nappendonlyy = true\n"))
		ast.NotNil(err)
	})

	t.Run("invalid", func(t *testing.T) {
		_, err := Load(write(t, "[aof]\nappendfsync = \"sometimes\"\n"))
		ast.NotNil(err)
		_, err = Load(write(t, "[tcp]\nport = 0\n"))
		ast.NotNil(err)
	})
}
