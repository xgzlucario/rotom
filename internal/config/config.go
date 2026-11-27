// Package config loads the rotom configuration file once at startup.
package config

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"

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
		AppendDirName  string `toml:"appenddirname"`  // directory of the multi-part AOF
		AppendFileName string `toml:"appendfilename"` // file name prefix inside it
		AppendFsync    string `toml:"appendfsync"`    // always | everysec | no

		// Rewrite automatically once the AOF has grown by this percentage
		// since the last rewrite and is at least AutoRewriteMinSize. 0 disables.
		AutoRewritePercentage int  `toml:"auto-aof-rewrite-percentage"`
		AutoRewriteMinSize    Size `toml:"auto-aof-rewrite-min-size"`
	} `toml:"aof"`
}

// Size is a byte count written like redis sizes: 1024, "64kb", "64mb", "1gb".
type Size int64

func (s *Size) UnmarshalText(text []byte) error {
	n, err := ParseSize(string(text))
	*s = Size(n)
	return err
}

// ParseSize parses a size with an optional b/kb/mb/gb suffix (case insensitive).
func ParseSize(s string) (int64, error) {
	str := strings.ToLower(strings.TrimSpace(s))
	unit := int64(1)
	for _, u := range []struct {
		suffix string
		mul    int64
	}{{"gb", 1 << 30}, {"mb", 1 << 20}, {"kb", 1 << 10}, {"b", 1}} {
		if strings.HasSuffix(str, u.suffix) {
			str, unit = strings.TrimSuffix(str, u.suffix), u.mul
			break
		}
	}
	n, err := strconv.ParseInt(str, 10, 64)
	if err != nil || n < 0 || n > (1<<62)/unit {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	return n * unit, nil
}

// Default returns the configuration used for missing fields.
func Default() *Config {
	c := &Config{}
	c.TCP.Port = 6379
	c.RDB.DBFileName = "dump.rdb"
	c.AOF.AppendDirName = "appendonlydir"
	c.AOF.AppendFileName = "appendonly.aof"
	c.AOF.AppendFsync = "everysec"
	c.AOF.AutoRewritePercentage = 100
	c.AOF.AutoRewriteMinSize = 64 << 20
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
	if c.AOF.AppendDirName == "" {
		return fmt.Errorf("aof.appenddirname must not be empty")
	}
	if c.AOF.AppendFileName == "" || strings.ContainsAny(c.AOF.AppendFileName, `/\`) {
		return fmt.Errorf("aof.appendfilename must be a plain file name")
	}
	if c.AOF.AutoRewritePercentage < 0 {
		return fmt.Errorf("aof.auto-aof-rewrite-percentage must not be negative")
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
	case "appenddirname":
		return c.AOF.AppendDirName, true
	case "appendfilename":
		return c.AOF.AppendFileName, true
	case "auto-aof-rewrite-percentage":
		return strconv.Itoa(c.AOF.AutoRewritePercentage), true
	case "auto-aof-rewrite-min-size":
		return strconv.FormatInt(int64(c.AOF.AutoRewriteMinSize), 10), true
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
