package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"
	"syscall"
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
	setLimit()
	debug()
	server.config = config
	server.RunServe()
}

func setLimit() {
	var rLimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
		panic(err)
	}
	rLimit.Cur = rLimit.Max
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
		panic(err)
	}

	log.Printf("set cur fd limit: %d", rLimit.Cur)
}
