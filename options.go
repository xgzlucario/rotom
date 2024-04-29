package rotom

import (
	"errors"
)

var DefaultOptions = Options{
	DirPath:          "rotom",
	ShardCount:       1024,
	RunSkipLoadError: true,
}

// Options represents the configuration for rotom.
type Options struct {
	// Dir path if the db storage path.
	DirPath string

	// ShardCount is the shard numbers for underlying hashmap.
	ShardCount uint32

	// Skip error when loading db file when startup.
	RunSkipLoadError bool
}

func checkOptions(option Options) error {
	if option.ShardCount == 0 {
		return errors.New("invalid shard count")
	}
	if option.DirPath == "" {
		return errors.New("invalid dir path")
	}
	return nil
}
