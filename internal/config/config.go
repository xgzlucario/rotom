// Package config loads the rotom configuration file once at startup.
package config

import (
	"bytes"
	"fmt"
	"os"
	"strconv"

	"github.com/pelletier/go-toml/v2"
)

type Config struct {
	TCP struct {
		Port int `toml:"port"`
	} `toml:"tcp"`

	RDB struct {
		Save       bool   `toml:"save"`
		DBFileName string `toml:"dbfilename"`
	} `toml:"rdb"`

	AOF struct {
		AppendOnly     bool   `toml:"appendonly"`
		AppendFileName string `toml:"appendfilename"`
		AppendFsync    string `toml:"appendfsync"` // always | everysec | no
	} `toml:"aof"`
}

// Default returns the configuration used for missing fields.
func Default() *Config {
	c := &Config{}
	c.TCP.Port = 6379
	c.RDB.DBFileName = "dump.rdb"
	c.AOF.AppendFileName = "appendonly.aof"
	c.AOF.AppendFsync = "everysec"
	return c
}

// Load reads a toml file on top of the defaults. Unknown keys are rejected,
// so that typos do not silently fall back to defaults.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	c := Default()
	dec := toml.NewDecoder(bytes.NewReader(data)).DisallowUnknownFields()
	if err := dec.Decode(c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return c, c.Validate()
}

func (c *Config) Validate() error {
	if c.TCP.Port <= 0 || c.TCP.Port > 65535 {
		return fmt.Errorf("invalid tcp.port %d", c.TCP.Port)
	}
	switch c.AOF.AppendFsync {
	case "always", "everysec", "no":
	default:
		return fmt.Errorf("invalid aof.appendfsync %q, want always|everysec|no", c.AOF.AppendFsync)
	}
	if c.RDB.DBFileName == "" {
		return fmt.Errorf("rdb.dbfilename must not be empty")
	}
	if c.AOF.AppendFileName == "" {
		return fmt.Errorf("aof.appendfilename must not be empty")
	}
	return nil
}

func yesno(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

// Get returns a parameter by its redis name, as used by CONFIG GET.
func (c *Config) Get(name string) (string, bool) {
	switch name {
	case "port":
		return strconv.Itoa(c.TCP.Port), true
	case "appendonly":
		return yesno(c.AOF.AppendOnly), true
	case "appendfilename":
		return c.AOF.AppendFileName, true
	case "appendfsync":
		return c.AOF.AppendFsync, true
	case "dbfilename":
		return c.RDB.DBFileName, true
	case "save":
		// rotom has no automatic save points
		return "", true
	}
	return "", false
}
