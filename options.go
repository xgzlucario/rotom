package rotom

import (
	"errors"
)

// SyncPolicy represents how often data is synced to disk.
type SyncPolicy byte

const (
	EverySec SyncPolicy = iota
	Always
)

var (
	DefaultOptions = Options{
		DirPath:          "rotom",
		ShardCount:       1024,
		SyncPolicy:       EverySec,
		ShrinkCronExpr:   "0 0 0/1 * * ?", // every hour.
		RunSkipLoadError: true,
	}
)

// Options represents the configuration for rotom.
type Options struct {
	// Dir path if the db storage path.
	DirPath string

	// ShardCount is the shard numbers for underlying hashmap.
	ShardCount uint32

	// SyncPolicy is whether to synchronize writes to disk.
	SyncPolicy SyncPolicy

	// ShrinkCronExpr sauto shrink will be triggered when cron expr is satisfied.
	// cron expression follows the standard cron expression.
	// e.g. "0 0 * * *" means merge at 00:00:00 every day.
	// Setting empty string "" will disable auto shrink.
	ShrinkCronExpr string

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
