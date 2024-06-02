package main

import (
	"log"
	"syscall"
)

func main() {
	var err error
	config, err = LoadConfig("config.json")
	if err != nil {
		log.Panicf("load config error: %v\n", err)
	}
	if err = InitDB(); err != nil {
		log.Panicf("init db error: %v\n", err)
	}
	setLimit()
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

	log.Printf("set cur socketFD limit: %d", rLimit.Cur)
}
