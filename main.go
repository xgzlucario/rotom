package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/rs/zerolog"
)

var (
	log       = initLogger()
	buildTime string
)

func initLogger() zerolog.Logger {
	return zerolog.
		New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.DateTime}).
		Level(zerolog.TraceLevel).
		With().
		Timestamp().
		Logger()
}

func config4Server(config *Config) {
	if err := initServer(config); err != nil {
		log.Fatal().Msgf("init server error: %v", err)
	}
	if err := InitDB(config); err != nil {
		log.Fatal().Msgf("init db error: %v", err)
	}
}

func printBanner(config *Config) {
	log.Printf(`
________      _____                  
___  __ \_______  /_____________ ___   Rotom %d bit (%s/%s)
__  /_/ /  __ \  __/  __ \_  __ '__ \  Port: %d, Pid: %d
_  _, _// /_/ / /_ / /_/ /  / / / / /  Build: %s
/_/ |_| \____/\__/ \____//_/ /_/ /_/
	   `,
		strconv.IntSize, runtime.GOARCH, runtime.GOOS,
		config.Port, os.Getpid(),
		buildTime)
}

// RegisterAeLoop register main aeLoop event.
func RegisterAeLoop(server *Server) {
	server.aeLoop.AddRead(server.fd, AcceptHandler, nil)
	server.aeLoop.AddTimeEvent(AE_NORMAL, 100, CronEvictExpired, nil)
	if server.config.AppendOnly {
		server.aeLoop.AddTimeEvent(AE_NORMAL, 1000, CronSyncAOF, nil)
	}
}

func main() {
	var path string
	var debug bool

	flag.StringVar(&path, "config", "config.json", "default config file path.")
	flag.BoolVar(&debug, "debug", false, "run with debug mode.")
	flag.Parse()

	config, err := LoadConfig(path)
	if err != nil {
		log.Fatal().Msgf("load config error: %v", err)
	}
	printBanner(config)

	if debug {
		go http.ListenAndServe(":6060", nil)
	}

	log.Info().Str("config", path).Msg("read config file")
	config4Server(config)

	log.Info().Msg("rotom server is ready to accept.")

	RegisterAeLoop(&server)
	server.aeLoop.AeMain()
}
