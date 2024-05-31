package main

import (
	"bytes"
	"fmt"
	"log"
	"strconv"

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

type RotomDB struct {
	strs   *cache.GigaCache
	extras map[string]any
	aof    *Aof
}

type RotomServer struct {
	gnet.BuiltinEventEngine
	engine  gnet.Engine
	clients map[int]*RotomClient
	config  *Config
}

type RotomClient struct {
	fd       int
	replyBuf *bytes.Buffer
	curCmd   string
	args     []Value
}

type RotomCommand struct {
	name    string
	handler func(*RotomClient)
	arity   int // arity represents the minimal number of arguments that command accepts.
	aofNeed bool
}

// global varibles
var (
	db       RotomDB
	server   RotomServer
	cmdTable []RotomCommand = []RotomCommand{
		{"ping", pingCommand, 0, false},
		{"set", setCommand, 2, true},
		{"get", getCommand, 1, false},
		{"hset", hsetCommand, 3, true},
		{"hget", hgetCommand, 2, false},
		{"hdel", hdelCommand, 2, true},
		{"hgetall", hgetallCommand, 1, false},
		{"lpush", lpushCommand, 2, true},
		{"rpush", rpushCommand, 2, true},
		// TODO
	}
)

func lookupCommand(cmdStr string) *RotomCommand {
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

	if config.AppendOnly {
		db.aof, err = NewAof(config.AppendFileName)
		if err != nil {
			log.Printf("failed to initialize aof file: %v\n", err)
			return
		}

		// Create client0 to process command from aof file.
		client0 := &RotomClient{
			fd:       0,
			replyBuf: bytes.NewBuffer(nil),
		}

		log.Printf("start loading aof file...")

		// Load the initial data into memory by processing each stored command.
		db.aof.Read(func(value Value) {
			command := value.array[0].bulk
			args := value.array[1:]

			client0.processCommand(command, args)
			client0.replyBuf.Reset()
		})
	}

	return nil
}

func Run(config *Config) error {
	server.config = config
	server.clients = make(map[int]*RotomClient)
	servePath := fmt.Sprintf("tcp://:%d", config.Port)
	log.Printf("rotom server is binding on %s\n", servePath)

	return gnet.Run(&server, servePath)
}

func (c *RotomClient) addReplyStr(s string) {
	c.replyBuf.WriteByte(STRING)
	c.replyBuf.WriteString(s)
	c.replyBuf.Write(CRLF)
}

func (c *RotomClient) addReplyBulk(b []byte) {
	c.replyBuf.WriteByte(BULK)
	c.replyBuf.WriteString(strconv.Itoa(len(b)))
	c.replyBuf.Write(CRLF)
	c.replyBuf.Write(b)
	c.replyBuf.Write(CRLF)
}

func (c *RotomClient) addReplyArrayBulk(b [][]byte) {
	c.replyBuf.WriteByte(BULK)
	c.replyBuf.WriteString(strconv.Itoa(len(b)))
	c.replyBuf.Write(CRLF)
	for _, val := range b {
		c.addReplyBulk(val)
	}
}

func (c *RotomClient) addReplyInteger(n int) {
	c.replyBuf.WriteByte(INTEGER)
	c.replyBuf.WriteString(strconv.Itoa(n))
	c.replyBuf.Write(CRLF)
}

func (c *RotomClient) addReplyError(err error) {
	c.replyBuf.WriteByte(ERROR)
	c.replyBuf.WriteString(err.Error())
	c.replyBuf.Write(CRLF)
}

func (c *RotomClient) addReplyNull() {
	c.replyBuf.WriteString("$-1")
	c.replyBuf.Write(CRLF)
}

func (c *RotomClient) addReplyWrongArgs() {
	c.addReplyError(fmt.Errorf("ERR wrong number of arguments for '%s' command", c.curCmd))
}

func (c *RotomClient) reset() {
	c.curCmd = ""
	c.replyBuf.Reset()
	c.args = c.args[:0]
}
