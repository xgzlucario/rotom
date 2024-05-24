package main

import (
	"bytes"
	"fmt"
	"log"
	"unsafe"

	"github.com/panjf2000/gnet/v2"
)

func (s *Server) OnBoot(eng gnet.Engine) gnet.Action {
	s.engine = eng
	return gnet.None
}

func (es *Server) OnTraffic(c gnet.Conn) gnet.Action {
	handleConnection(c)
	return gnet.None
}

func main() {
	config, err := LoadConfig("config.json")
	if err != nil {
		log.Printf("config error: %v\n", err)
	}
	err = initServer(config)
	if err != nil {
		log.Printf("init server error: %v\n", err)
	}
	log.Println("rotom server is up.")
	server.Run()
}

func handleConnection(conn gnet.Conn) {
	resp := NewResp(conn)
	for {
		value, err := resp.Read()
		if err != nil {
			return
		}

		if value.typ != TypeArray || len(value.array) == 0 {
			fmt.Println("Invalid request, expected non-empty array")
			continue
		}

		command := bytes.ToLower(value.array[0].bulk)
		args := value.array[1:]

		processCommand(conn, command, args)
	}
}

func processCommand(conn gnet.Conn, cmdStr []byte, args []Value) {
	writer := NewWriter(conn)

	cmd := lookupCommand(b2s(cmdStr))
	if cmd == nil {
		log.Printf("Invalid command: %s\n", cmdStr)
		writer.Write(Value{typ: TypeString, str: nil})
		return
	}

	// check command args
	if len(args) < 2 {
		result := Value{
			typ: TypeError,
			str: []byte(fmt.Sprintf("ERR wrong number of arguments for '%s' command", cmd.name)),
		}
		writer.Write(result)
		return
	}

	if b2s(cmdStr) == "set" || b2s(cmdStr) == "hset" {
		// Manually constructing the array slice to include command and args.
		values := make([]Value, len(args)+1)
		values[0] = Value{typ: TypeBulk, bulk: cmdStr}
		copy(values[1:], args)
		server.db.aof.Write(Value{typ: TypeArray, array: values})
	}

	result := cmd.handler(args)
	writer.Write(result)
}

func b2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
