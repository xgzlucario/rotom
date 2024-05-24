package main

import (
	"fmt"
	"log"

	"github.com/panjf2000/gnet/v2"
	cache "github.com/xgzlucario/GigaCache"
)

type DB struct {
	strs   *cache.GigaCache
	extras map[string]any
	aof    *Aof
}

type Server struct {
	gnet.BuiltinEventEngine
	engine gnet.Engine
	port   int
	db     *DB
}

// type Client struct {
// 	conn net.Conn
// }

type CommandHandler func([]Value) Value

type Command struct {
	name    string
	handler CommandHandler
	arity   int
}

// global varibles
var (
	server   Server
	cmdTable []Command = []Command{
		{"ping", pingCommand, 0},
		{"set", setCommand, 2},
		{"get", getCommand, 1},
		{"hset", hsetCommand, 3},
		{"hget", hgetCommand, 2},
		{"hgetall", hgetallCommand, 1},
		// TODO
	}
)

func lookupCommand(cmdStr string) *Command {
	for _, c := range cmdTable {
		if c.name == cmdStr {
			return &c
		}
	}
	return nil
}

func initServer(config *Config) (err error) {
	server.port = config.Port
	server.db = &DB{
		strs:   cache.New(cache.DefaultOptions),
		extras: map[string]any{},
	}
	server.db.aof, err = NewAof(config.AppendOnlyFileName)
	if err != nil {
		log.Printf("failed to initialize aof file: %v\n", err)
		return
	}
	return nil
}

func (s *Server) Run() {
	defer server.db.aof.Close()

	servePath := fmt.Sprintf("tcp://:%d", s.port)
	gnet.Run(&server, servePath)
}
