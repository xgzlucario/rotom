package main

import (
	"fmt"
	"log"

	"github.com/panjf2000/gnet/v2"
	cache "github.com/xgzlucario/GigaCache"
	"github.com/xgzlucario/rotom/structx"
)

// DataType is the data type for Rotom.
type DataType byte

const (
	TypeMap DataType = iota + 1
	TypeSet
	TypeList
	TypeZSet
	TypeBitmap
)

// Type aliases for built-in types.
type (
	Map    = *structx.Map
	Set    = *structx.Set
	List   = *structx.List
	ZSet   = *structx.ZSet
	BitMap = *structx.Bitmap
)

type DB struct {
	strs   *cache.GigaCache
	extras map[string]any
	aof    *Aof
}

type Server struct {
	gnet.BuiltinEventEngine
	engine gnet.Engine
	config *Config
}

type CommandHandler func([]Value) Value

type Command struct {
	name    string
	handler CommandHandler
	arity   int
}

// global varibles
var (
	db       DB
	server   Server
	cmdTable []Command = []Command{
		{"ping", pingCommand, 0},
		{"set", setCommand, 2},
		{"get", getCommand, 1},
		{"hset", hsetCommand, 3},
		{"hget", hgetCommand, 2},
		{"hdel", hdelCommand, 2},
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

func initDB(config *Config) (err error) {
	db.strs = cache.New(cache.DefaultOptions)
	db.extras = make(map[string]any)
	db.aof, err = NewAof(config.AppendOnlyFileName)
	if err != nil {
		log.Printf("failed to initialize aof file: %v\n", err)
		return
	}
	return nil
}

func RunServer(config *Config) (err error) {
	server.config = config
	defer db.aof.Close()

	servePath := fmt.Sprintf("tcp://:%d", config.Port)
	log.Printf("rotom server is binding on %s\n", servePath)
	return gnet.Run(&server, servePath)
}
