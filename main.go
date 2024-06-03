package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"
)

func debug() {
	go http.ListenAndServe(":6060", nil)
}

func main() {
	var err error
	config, err := LoadConfig("config.json")
	if err != nil {
		log.Panicf("load config error: %v\n", err)
	}
	if err = InitDB(config); err != nil {
		log.Panicf("init db error: %v\n", err)
	}
	debug()
	server.config = config
	server.RunServe()
}
