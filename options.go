package rotom

import (
	"errors"
	"time"
)

// SyncPolicy represents how often data is synced to disk.
type SyncPolicy byte

const (
	EverySecond SyncPolicy = iota
	// TODO: Sync, Never
)

var (
	DefaultOptions = Options{
		DirPath:          "rotom",
		ShardCount:       1024,
		SyncPolicy:       EverySecond,
		ShrinkInterval:   time.Minute,
		RunSkipLoadError: true,
	}
)

// Options represents the configuration for a Store.
type Options struct {
	// ShardCount is the shard numbers to underlying GigaCache used.
	ShardCount uint32

	// Dir path if the db storage path.
	DirPath string

	// Data sync policy.
	SyncPolicy SyncPolicy

	// Shrink db file interval.
	ShrinkInterval time.Duration

	// Starts when loading db file error.
	RunSkipLoadError bool
}

// checkOptions checks the validity of the options.
func checkOptions(option Options) error {
	if option.ShardCount == 0 {
		return errors.New("invalid shard count")
	}
	if option.DirPath == "" {
		return errors.New("invalid dir path")
	}
	return nil
}
