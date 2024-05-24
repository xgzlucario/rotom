package main

import (
	"bytes"
	"fmt"
	"log"
	"unsafe"

	"github.com/panjf2000/gnet/v2"
)

var aof *Aof

type RotomServer struct {
	gnet.BuiltinEventEngine
	eng  gnet.Engine
	addr string
}

func (es *RotomServer) OnBoot(eng gnet.Engine) gnet.Action {
	es.eng = eng
	log.Println("echo server is listening on", es.addr)
	return gnet.None
}

func (es *RotomServer) OnTraffic(c gnet.Conn) gnet.Action {
	handleConnection(c)
	return gnet.None
}

func main() {
	var err error
	aof, err = NewAof("database.aof")
	if err != nil {
		fmt.Println("Failed to initialize AOF:", err)
		return
	}
	defer aof.Close()

	port := 9006

	server := &RotomServer{addr: fmt.Sprintf("tcp://:%d", port)}
	log.Fatal(gnet.Run(server, server.addr))
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

		command := bytes.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		processCommand(conn, command, args)
	}
}

func processCommand(conn gnet.Conn, command []byte, args []Value) {
	writer := NewWriter(conn)

	handler, ok := Handlers[b2s(command)]
	if !ok {
		log.Printf("Invalid command: %s\n", command)
		writer.Write(Value{typ: TypeString, str: nil})
		return
	}

	// Handle special commands like "SET" or "HSET" that modify the database.
	if b2s(command) == "SET" || b2s(command) == "HSET" {
		// Manually constructing the array slice to include command and args.
		values := make([]Value, len(args)+1)
		values[0] = Value{typ: TypeBulk, bulk: command}
		copy(values[1:], args)
		aof.Write(Value{typ: TypeArray, array: values})
	}

	result := handler(args)
	writer.Write(result)
}

func b2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
