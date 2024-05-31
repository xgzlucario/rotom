package main

import (
	"bytes"
	"log"

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
		log.Printf("load config error: %v\n", err)
	}
	if err = initDB(config); err != nil {
		log.Printf("init db error: %v\n", err)
	}
	if err = Run(config); err != nil {
		log.Printf("init server error: %v\n", err)
	}
}

func handleConnection(conn gnet.Conn) {
	fd := conn.Fd()
	client, ok := server.clients[fd]
	if !ok {
		client = &RotomClient{
			fd:       fd,
			replyBuf: bytes.NewBuffer(make([]byte, 0, 16)),
		}
		server.clients[fd] = client

		log.Printf("connected to new client:%d", fd)
	}

	resp := NewResp(conn)
	for {
		value, err := resp.Read()
		if err != nil {
			return
		}

		if value.typ != TypeArray || len(value.array) == 0 {
			log.Println("invalid request, expected non-empty array")
			continue
		}

		command := bytes.ToLower(value.array[0].bulk)
		args := value.array[1:]

		client.processCommand(command, args)

		if err = conn.AsyncWrite(client.replyBuf.Bytes(), nil); err != nil {
			log.Printf("async write reply error: %v", err)
		}
		client.reset()
	}
}

func (c *RotomClient) processCommand(cmdStr []byte, args []Value) {
	c.curCmd = b2s(cmdStr)
	cmd := lookupCommand(c.curCmd)
	if cmd == nil {
		log.Printf("invalid command: %s\n", cmdStr)
		c.addReplyNull()
		return
	}

	// Check command args.
	if len(args) < cmd.arity {
		c.addReplyWrongArgs()
		return
	}
	c.args = args

	// Write aof file if needed.
	if server.config.AppendOnly && cmd.aofNeed && c.fd > 0 {
		args := append([]Value{{typ: TypeBulk, bulk: cmdStr}}, args...)
		db.aof.Write(Value{typ: TypeArray, array: args})
	}

	cmd.handler(c)
}
