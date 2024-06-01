package main

import (
	"bytes"
	"log"
	"net"
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
	server.Run()
}

func handleConnection(buf []byte, conn net.Conn) {
	resp := NewResp(bytes.NewReader(buf))
	for {
		value, err := resp.Read()
		if err != nil {
			return
		}

		if value.typ != ARRAY || len(value.array) == 0 {
			log.Println("invalid request, expected non-empty array")
			continue
		}

		value.array[0].bulk = bytes.ToLower(value.array[0].bulk)
		command := value.array[0].bulk
		args := value.array[1:]

		var res Value

		// Lookup for command.
		cmd, err := lookupCommand(b2s(command))
		if err != nil {
			log.Printf("%v", err)
			res = NewErrValue(err)

		} else {
			// Write aof file if needed.
			if config.AppendOnly {
				cmd.writeAofFile(db.aof, value.array)
			}

			// Process command.
			res = cmd.processCommand(args)
		}

		// Async write result.
		go func() {
			if _, err = conn.Write(res.Marshal()); err != nil {
				log.Printf("write reply error: %v", err)
			}
		}()
	}
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

	log.Printf("set cur limit: %d", rLimit.Cur)
}
