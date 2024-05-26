package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
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
	conn     net.Conn
	replyBuf *bytes.Buffer
	rawargs  []Value // store origin args from client
	args     []Value // store args except bulk cmdStr
}

type RotomCommand struct {
	name    string
	handler func(*RotomClient)
	arity   int
}

// global varibles
var (
	db       RotomDB
	server   RotomServer
	cmdTable []RotomCommand = []RotomCommand{
		{"ping", pingCommand, 0},
		{"set", setCommand, 2},
		{"get", getCommand, 1},
		{"hset", hsetCommand, 3},
		{"hget", hgetCommand, 2},
		{"hdel", hdelCommand, 2},
		{"hgetall", hgetallCommand, 1},
		{"expire", expireCommand, 2},
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
	db.aof, err = NewAof(config.AppendFileName)
	if err != nil {
		log.Printf("failed to initialize aof file: %v\n", err)
		return
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

func (c *RotomClient) addReplyInteger(n int64) {
	c.replyBuf.WriteByte(INTEGER)
	c.replyBuf.WriteString(strconv.FormatInt(n, 10))
	c.replyBuf.Write(CRLF)
}

func (c *RotomClient) addReplyError(err error) {
	c.replyBuf.WriteByte(ERROR)
	c.replyBuf.WriteString(err.Error())
	c.replyBuf.Write(CRLF)
}

func (c *RotomClient) addReplyNull() {
	c.write([]byte("$-1\r\n"))
}

func (c *RotomClient) write(b []byte) {
	_, err := c.conn.Write(b)
	if err != nil {
		log.Printf("Client write buffer error: %v\n", err)
	}
}
