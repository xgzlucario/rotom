package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/rs/zerolog"
)

var (
	log       = initLogger()
	buildTime string
)

func runDebug() {
	go http.ListenAndServe(":6060", nil)
}

func initLogger() zerolog.Logger {
	return zerolog.
		New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.DateTime}).
		Level(zerolog.TraceLevel).
		With().
		Timestamp().
		Logger()
}

func main() {
	var path string
	var debug bool

	flag.StringVar(&path, "config", "config.json", "default config file path.")
	flag.BoolVar(&debug, "debug", false, "run with debug mode.")
	flag.Parse()

	log.Info().Str("buildTime", buildTime).Msg("current version")
	log.Info().Str("config", path).Bool("debug", debug).Msg("read cmd arguments")

	config, err := LoadConfig(path)
	if err != nil {
		log.Fatal().Msgf("load config error: %v", err)
	}
	if err = initServer(config); err != nil {
		log.Fatal().Msgf("init server error: %v", err)
	}
	if err = InitDB(config); err != nil {
		log.Fatal().Msgf("init db error: %v", err)
	}
	if debug {
		runDebug()
	}

	log.Info().Int("port", config.Port).Msg("running on")
	log.Info().Msg("rotom server is ready to accept.")

	// register main aeLoop event
	server.aeLoop.AddRead(server.fd, AcceptHandler, nil)
	server.aeLoop.AddTimeEvent(AE_NORMAL, 100, EvictExpired, nil)
	if server.config.AppendOnly {
		server.aeLoop.AddTimeEvent(AE_NORMAL, 1000, SyncAOF, nil)
	}
	server.aeLoop.AeMain()
}
