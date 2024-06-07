package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/rs/zerolog"
)

var logger = zerolog.
	New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.DateTime}).
	Level(zerolog.TraceLevel).
	With().
	Timestamp().
	Logger()

func runDebug() {
	go http.ListenAndServe(":6060", nil)
}

func main() {
	var path string
	var debug bool

	flag.StringVar(&path, "config", "config.json", "default config file path.")
	flag.BoolVar(&debug, "debug", false, "run with debug mode.")
	flag.Parse()

	logger.Debug().Str("config", path).Bool("debug", debug).Msg("read cmd arguments")

	config, err := LoadConfig(path)
	if err != nil {
		logger.Error().Msgf("load config error: %v", err)
	}
	if err = initServer(config); err != nil {
		logger.Error().Msgf("init server error: %v", err)
	}
	if err = InitDB(config); err != nil {
		logger.Error().Msgf("init db error: %v", err)
	}
	if debug {
		runDebug()
	}
	server.aeLoop.AddFileEvent(server.fd, AE_READABLE, AcceptHandler, nil)
	// server.aeLoop.AddTimeEvent(AE_NORMAL, 100, ServerCron, nil)
	logger.Debug().Msg("rotom server is ready to accept.")
	server.aeLoop.AeMain()
}
