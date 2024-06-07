package main

import (
	"bytes"
	"container/list"
	"fmt"
	"strings"

	cache "github.com/xgzlucario/GigaCache"
	"github.com/xgzlucario/rotom/structx"
)

const (
	IO_BUF = 64 * KB
)

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

type Client struct {
	fd       int
	queryLen int
	queryBuf []byte
	reply    *list.List
}

type Server struct {
	fd      int
	config  *Config
	aeLoop  *AeLoop
	clients map[int]*Client
}

type Command struct {
	// name is command string name.
	// it should consist of all lowercase letters.
	name string

	// handler is this command real database handler function.
	handler func([]Value) Value

	// arity represents the minimal number of arguments that command accepts.
	arity int

	// persist indicates whether this command needs to be persisted.
	// effective when `appendonly` is true.
	persist bool
}

var (
	// db is the main database object.
	db DB

	// server is the main server object.
	server Server

	// cmdTable is the list of all available commands.
	cmdTable []Command = []Command{
		{"ping", pingCommand, 0, false},
		{"set", setCommand, 2, true},
		{"get", getCommand, 1, false},
		{"hset", hsetCommand, 3, true},
		{"hget", hgetCommand, 2, false},
		{"hdel", hdelCommand, 2, true},
		{"hgetall", hgetallCommand, 1, false},
	}
)

func lookupCommand(command string) *Command {
	cmdStr := strings.ToLower(command)
	for _, c := range cmdTable {
		if c.name == cmdStr {
			return &c
		}
	}
	return nil
}

func (cmd *Command) processCommand(args []Value) Value {
	if len(args) < cmd.arity {
		return newErrValue(ErrWrongNumberArgs(cmd.name))
	}
	return cmd.handler(args)
}

// InitDB initializes database and redo appendonly files if nedded.
func InitDB(config *Config) (err error) {
	options := cache.DefaultOptions
	options.ConcurrencySafe = false
	db.strs = cache.New(options)
	db.extras = make(map[string]any)

	if config.AppendOnly {
		db.aof, err = NewAof(config.AppendFileName)
		if err != nil {
			logger.Error().Msgf("failed to initialize aof file: %v", err)
			return
		}

		logger.Debug().Msg("start loading aof file...")

		// Load the initial data into memory by processing each stored command.
		err = db.aof.Read(func(value Value) {
			command := string(value.array[0].bulk)
			args := value.array[1:]

			cmd := lookupCommand(command)
			if cmd != nil {
				cmd.processCommand(args)
			}
		})
		if err != nil {
			logger.Error().Msgf("read appendonly file error: %v", err)
			return
		}
	}

	return nil
}

// AcceptHandler is the main file event of aeloop.
func AcceptHandler(loop *AeLoop, fd int, _ interface{}) {
	cfd, err := Accept(fd)
	if err != nil {
		logger.Error().Msgf("accept err: %v", err)
		return
	}
	// create client
	client := &Client{
		fd:       cfd,
		reply:    list.New(),
		queryBuf: make([]byte, KB),
	}
	server.clients[cfd] = client
	loop.AddFileEvent(cfd, AE_READABLE, ReadQueryFromClient, client)
	logger.Debug().Msgf("accept client, fd: %d", cfd)
}

func ReadQueryFromClient(loop *AeLoop, fd int, extra interface{}) {
	client := extra.(*Client)
	n, err := Read(fd, client.queryBuf[client.queryLen:])
	if err != nil {
		logger.Error().Msgf("client %v read err: %v", fd, err)
		freeClient(client)
		return
	}
	if n > 0 {
		client.queryLen += n
		ProcessQueryBuf(client)
	}
}

func resetClient(client *Client) {
	client.queryLen = 0
}

func freeClient(client *Client) {
	delete(server.clients, client.fd)
	server.aeLoop.RemoveFileEvent(client.fd, AE_READABLE)
	server.aeLoop.RemoveFileEvent(client.fd, AE_WRITABLE)
	Close(client.fd)
}

func ProcessQueryBuf(client *Client) {
	queryBuf := client.queryBuf[:client.queryLen]
	resp := NewResp(bytes.NewReader(queryBuf))

	value, err := resp.Read()
	if err != nil {
		logger.Error().Msgf("read resp error: %v", err)
		return
	}

	command := value.array[0].bulk
	args := value.array[1:]
	var res Value

	// look up for command
	cmd := lookupCommand(string(command))
	if cmd != nil {
		res = cmd.processCommand(args)
		if server.config.AppendOnly && cmd.persist && res.typ != ERROR {
			db.aof.Write(queryBuf)
		}

	} else {
		res = newErrValue(fmt.Errorf("invalid command: %s", command))
	}

	client.reply.PushBack(res)
	resetClient(client)

	// ADD writable event
	server.aeLoop.AddFileEvent(client.fd, AE_WRITABLE, SendReplyToClient, client)
}

func SendReplyToClient(loop *AeLoop, fd int, extra interface{}) {
	client := extra.(*Client)

	// send all replies back
	for client.reply.Len() > 0 {
		elem := client.reply.Remove(client.reply.Front())
		_, err := Write(fd, elem.(Value).Marshal())
		if err != nil {
			logger.Error().Msgf("send reply err: %v", err)
			freeClient(client)
			return
		}
	}
	loop.RemoveFileEvent(fd, AE_WRITABLE)
}

func initServer(config *Config) (err error) {
	server.config = config
	server.clients = make(map[int]*Client)
	server.aeLoop, err = AeLoopCreate()
	if err != nil {
		return err
	}
	server.fd, err = TcpServer(config.Port)
	if err != nil {
		Close(server.fd)
		return err
	}
	return nil
}

// ServerCronFlush flush aof file for every second.
func ServerCronFlush(loop *AeLoop, id int, extra interface{}) {
	err := db.aof.Flush()
	logger.Debug().Msg("flush")
	if err != nil {
		logger.Error().Msgf("flush aof buffer error: %v", err)
	}
}
