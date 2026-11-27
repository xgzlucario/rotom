package server

import (
	"os"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func TestMain(m *testing.M) {
	// set once, before any server goroutine starts: changing the global
	// logger later would race with them.
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	os.Exit(m.Run())
}
