package main

import (
	"fmt"

	cache "github.com/xgzlucario/GigaCache"
	"github.com/xgzlucario/rotom/structx"
)

type CmdType = byte

const (
	COMMAND_UNKNOWN CmdType = iota
	COMMAND_INLINE
	COMMAND_BULK
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
	queryBuf []byte
	reply    []Value
}

type Server struct {
	fd      int
	port    int
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

func lookupCommand(command []byte) (*Command, error) {
	cmdStr := string(command)
	for _, c := range cmdTable {
		if c.name == cmdStr {
			return &c, nil
		}
	}
	return nil, fmt.Errorf("invalid command: %s", command)
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
			command := value.array[0].bulk
			args := value.array[1:]

			cmd, err := lookupCommand(command)
			if err == nil {
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

// func (server *Server) handleConnection(buf []byte, conn net.Conn) {
// resp := NewResp(buf)
// for {
// 	err := resp.Read(&server.args)
// 	if err != nil {
// 		if err != io.EOF {
// 			log.Println("read resp error:", err)
// 		}
// 		return
// 	}

// 	if server.args.typ != ARRAY || len(server.args.array) == 0 {
// 		log.Println("invalid request, expected non-empty array")
// 		continue
// 	}

// 	command := server.args.array[0].bulk
// 	args := server.args.array[1:]
// 	var res Value

// 	cmd, err := lookupCommand(command)
// 	if err != nil {
// 		res = newErrValue(err)

// 	} else {
// 		res = cmd.processCommand(args)

// 		if server.config.AppendOnly && cmd.persist && res.typ != ERROR {
// 			db.aof.Write(buf)
// 		}
// 	}

// 	if _, err = conn.Write(res.Marshal()); err != nil {
// 		log.Println("write reply error:", err)
// 	}
// }
// }

// AcceptHandler is the main file event of aeloop.
func AcceptHandler(loop *AeLoop, fd int, extra interface{}) {
	cfd, err := Accept(fd)
	if err != nil {
		logger.Error().Msgf("accept err: %v", err)
		return
	}
	client := CreateClient(cfd)
	server.clients[cfd] = client
	server.aeLoop.AddFileEvent(cfd, AE_READABLE, ReadQueryFromClient, client)
	logger.Debug().Msgf("accept client, fd: %d", cfd)
}

func CreateClient(fd int) *Client {
	var client Client
	client.fd = fd
	client.reply = make([]Value, 0, 8)
	client.queryBuf = make([]byte, KB)
	return &client
}

func ReadQueryFromClient(loop *AeLoop, fd int, extra interface{}) {
	client := extra.(*Client)
	_, err := Read(fd, client.queryBuf)
	if err != nil {
		logger.Error().Msgf("client %v read err: %v", fd, err)
		server.freeClient(client)
		return
	}

	// TODO
	ProcessQueryBuf(client)
}

func (server *Server) freeClient(client *Client) {
	delete(server.clients, client.fd)
	server.aeLoop.RemoveFileEvent(client.fd, AE_READABLE)
	server.aeLoop.RemoveFileEvent(client.fd, AE_WRITABLE)
	Close(client.fd)
}

func ProcessQueryBuf(c *Client) {
	// ALWAYS return ok
	c.reply = append(c.reply, ValueOK)
	// ADD writable event
	server.aeLoop.AddFileEvent(c.fd, AE_WRITABLE, SendReplyToClient, c)
}

func SendReplyToClient(loop *AeLoop, fd int, extra interface{}) {
	client := extra.(*Client)

	// send all replies back
	for _, reply := range client.reply {
		_, err := Write(fd, reply.Marshal())
		if err != nil {
			logger.Error().Msgf("send reply err: %v", err)
			server.freeClient(client)
			return
		}
	}
	client.reply = client.reply[:0]

	// remove file event
	loop.RemoveFileEvent(fd, AE_WRITABLE)
}

func initServer(config *Config) (err error) {
	server.port = config.Port
	server.clients = make(map[int]*Client)
	server.aeLoop, err = AeLoopCreate()
	if err != nil {
		return err
	}
	server.fd, err = TcpServer(server.port)
	if err != nil {
		Close(server.fd)
		return err
	}
	return nil
}
