package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"unsafe"

	"github.com/panjf2000/gnet/v2"
)

func (s *Server) OnBoot(engine gnet.Engine) gnet.Action {
	s.engine = engine
	return gnet.None
}

func (s *Server) OnTraffic(c gnet.Conn) gnet.Action {
	handleConnection(c)
	return gnet.None
}

func main() {
	config, err := LoadConfig("config.json")
	if err != nil {
		log.Printf("load config error: %v\n", err)
	}
	if err = initDB(config); err != nil {
		log.Printf("init db error: %v\n", err)
	}
	if err = RunServer(config); err != nil {
		log.Printf("init server error: %v\n", err)
	}
}

func handleConnection(conn net.Conn) {
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

func processCommand(conn net.Conn, cmdStr []byte, args []Value) {
	writer := NewWriter(conn)

	cmd := lookupCommand(b2s(cmdStr))
	if cmd == nil {
		log.Printf("Invalid command: %s\n", cmdStr)
		writer.Write(Value{typ: TypeString, str: nil})
		return
	}

	// check command args
	if len(args) < cmd.arity {
		result := Value{
			typ: TypeError,
			str: []byte(fmt.Sprintf("ERR wrong number of arguments for '%s' command", cmd.name)),
		}
		writer.Write(result)
		return
	}

	if b2s(cmdStr) == "set" || b2s(cmdStr) == "hset" || b2s(cmdStr) == "hdel" {
		// manually constructing the array slice to include command and args
		values := make([]Value, len(args)+1)
		values[0] = Value{typ: TypeBulk, bulk: cmdStr}
		copy(values[1:], args)
		db.aof.Write(Value{typ: TypeArray, array: values})
	}

	result := cmd.handler(args)
	writer.Write(result)
}

func b2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
