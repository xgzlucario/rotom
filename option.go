package rotom

import (
	"log/slog"
	"time"
)

// SyncPolicy represents how often data is synced to disk.
type SyncPolicy byte

const (
	Never SyncPolicy = iota
	EverySecond
	// TODO: Sync
)

var (
	// Default config for db
	DefaultConfig = Config{
		Path:             "rotom.db",
		ShardCount:       1024,
		SyncPolicy:       EverySecond,
		ShrinkInterval:   time.Minute,
		RunSkipLoadError: true,
		Logger:           slog.Default(),
	}

	// No persistent config
	NoPersistentConfig = Config{
		ShardCount: 1024,
		SyncPolicy: Never,
		Logger:     slog.Default(),
	}
)

// Config represents the configuration for a Store.
type Config struct {
	ShardCount int

	Path string // Path of db file.

	SyncPolicy     SyncPolicy    // Data sync policy.
	ShrinkInterval time.Duration // Shrink db file interval.

	RunSkipLoadError bool // Starts when loading db file error.

	Logger *slog.Logger // Logger for db, set <nil> if you don't want to use it.
}
