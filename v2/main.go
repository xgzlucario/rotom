package main

import (
	"bytes"
	"log"
	"unsafe"

	"github.com/panjf2000/gnet/v2"
)

func (s *RotomServer) OnBoot(engine gnet.Engine) gnet.Action {
	s.engine = engine
	return gnet.None
}

func (s *RotomServer) OnTraffic(c gnet.Conn) gnet.Action {
	handleConnection(c)
	return gnet.None
}

func main() {
	config, err := LoadConfig("config.json")
	if err != nil {
		log.Printf("Load config error: %v\n", err)
	}
	if err = initDB(config); err != nil {
		log.Printf("Init db error: %v\n", err)
	}
	if err = Run(config); err != nil {
		log.Printf("Init server error: %v\n", err)
	}
}

func handleConnection(conn gnet.Conn) {
	fd := conn.Fd()
	client, ok := server.clients[fd]
	if !ok {
		log.Printf("connected to new client:%d", fd)
		// create new client
		client = &RotomClient{
			fd:       fd,
			conn:     conn,
			replyBuf: bytes.NewBuffer(make([]byte, 0, 16)),
		}
		server.clients[fd] = client
	}

	resp := NewResp(conn)
	for {
		value, err := resp.Read()
		if err != nil {
			return
		}

		if value.typ != TypeArray || len(value.array) == 0 {
			log.Println("Invalid request, expected non-empty array")
			continue
		}

		command := bytes.ToLower(value.array[0].bulk)
		args := value.array[1:]

		client.processCommand(command, args)
	}
}

func (c *RotomClient) processCommand(cmdStr []byte, args []Value) {
	cmd := lookupCommand(b2s(cmdStr))
	if cmd == nil {
		log.Printf("Invalid command: %s\n", cmdStr)
		c.addReplyNull()
		goto WRITE
	}

	c.rawargs = append([]Value{{typ: TypeBulk, bulk: cmdStr}}, args...)
	c.args = c.rawargs[1:]

	// check command args
	if len(args) < cmd.arity {
		c.addReplyWrongNumberArgs()
		goto WRITE
	}

	cmd.handler(c)

WRITE:
	_, err := c.replyBuf.WriteTo(c.conn)
	if err != nil {
		log.Printf("Write reply error: %v", err)
	}
}

func b2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
