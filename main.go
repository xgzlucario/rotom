package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
)

func runDebug() {
	go http.ListenAndServe(":6060", nil)
}

func main() {
	var path string
	var debug bool

	flag.StringVar(&path, "config", "config.json", "default config file path.")
	flag.BoolVar(&debug, "debug", false, "run with debug mode.")
	flag.Parse()

	log.Printf("cmd arguments: config=%s, debug=%v", path, debug)

	config, err := LoadConfig(path)
	if err != nil {
		log.Panicf("load config error: %v\n", err)
	}
	if err = initServer(config); err != nil {
		log.Panicf("init server error: %v\n", err)
	}
	if err = InitDB(config); err != nil {
		log.Panicf("init db error: %v\n", err)
	}
	if debug {
		runDebug()
	}
	server.aeLoop.AddFileEvent(server.fd, AE_READABLE, AcceptHandler, nil)
	// server.aeLoop.AddTimeEvent(AE_NORMAL, 100, ServerCron, nil)
	log.Println("rotom server is up.")
	server.aeLoop.AeMain()
}
