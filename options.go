package rotom

import (
	"errors"
)

// SyncPolicy represents how often data is synced to disk.
type SyncPolicy byte

const (
	EverySecond SyncPolicy = iota
	Sync
)

var (
	DefaultOptions = Options{
		DirPath:          "rotom",
		ShardCount:       1024,
		SyncPolicy:       EverySecond,
		ShrinkCronExpr:   "0 0 0/1 * * ?", // every hour default
		RunSkipLoadError: true,
	}
)

// Options represents the configuration for rotom.
type Options struct {
	// Dir path if the db storage path.
	DirPath string

	// ShardCount is the shard numbers for underlying hashmap.
	ShardCount uint32

	// SyncPolicy
	SyncPolicy SyncPolicy

	// ShrinkCronExpr
	// auto shrink will be triggered when cron expr is satisfied.
	// cron expression follows the standard cron expression.
	// e.g. "0 0 * * *" means merge at 00:00:00 every day.
	ShrinkCronExpr string

	// Starts when loading db file error.
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
