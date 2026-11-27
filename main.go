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
	"github.com/rs/zerolog/log"
	"github.com/xgzlucario/rotom/internal/config"
	"github.com/xgzlucario/rotom/internal/server"
)

var buildTime string

func printBanner(cfg *config.Config) {
	log.Printf(`
________      _____                  
___  __ \_______  /_____________ ___   Rotom %d bit (%s/%s)
__  /_/ /  __ \  __/  __ \_  __ '__ \  Port: %d, Pid: %d
_  _, _// /_/ / /_ / /_/ /  / / / / /  Build: %s
/_/ |_| \____/\__/ \____//_/ /_/ /_/
	   `,
		strconv.IntSize, runtime.GOARCH, runtime.GOOS,
		cfg.TCP.Port, os.Getpid(),
		buildTime)
}

func main() {
	var path string
	var debug bool

	flag.StringVar(&path, "conf", "rotom.toml", "config file path.")
	flag.BoolVar(&debug, "debug", false, "run with debug mode (debug logs, pprof on :6060).")
	flag.Parse()

	level := zerolog.InfoLevel
	if debug {
		level = zerolog.DebugLevel
	}
	log.Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.DateTime}).
		Level(level).With().Timestamp().Logger()

	cfg, err := config.Load(path)
	if err != nil {
		log.Fatal().Msgf("load config error: %v", err)
	}
	srv, err := server.New(cfg)
	if err != nil {
		log.Fatal().Msgf("init server error: %v", err)
	}
	if err := srv.Listen(); err != nil {
		log.Fatal().Msgf("listen error: %v", err)
	}
	printBanner(cfg)

	if debug {
		go http.ListenAndServe(":6060", nil)
	}

	log.Info().Msg("rotom server is ready to accept.")
	srv.Run()
}
