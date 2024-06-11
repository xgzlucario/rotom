package main

import (
	"io"

	cache "github.com/xgzlucario/GigaCache"
	"github.com/xgzlucario/rotom/structx"
)

const (
	DEFAULT_IO_BUF = 16 * KB
	MAX_BULK       = 4 * KB
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
	reply    []Value
}

type Server struct {
	fd      int
	config  *Config
	aeLoop  *AeLoop
	clients map[int]*Client
}

var (
	db     DB
	server Server
)

// InitDB initializes database and redo appendonly files if nedded.
func InitDB(config *Config) (err error) {
	options := cache.DefaultOptions
	options.ConcurrencySafe = false
	options.EvictInterval = 5
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
			command := value.array[0].ToString()
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
		reply:    make([]Value, 0, 8),
		queryBuf: make([]byte, DEFAULT_IO_BUF),
	}
	server.clients[cfd] = client
	loop.AddFileEvent(cfd, AE_READABLE, ReadQueryFromClient, client)
	logger.Debug().Msgf("accept client, fd: %d", cfd)
}

func ReadQueryFromClient(loop *AeLoop, fd int, extra interface{}) {
	client := extra.(*Client)

	// grow query buffer
	if len(client.queryBuf)-client.queryLen < MAX_BULK {
		client.queryBuf = append(client.queryBuf, make([]byte, MAX_BULK)...)
	}

	n, err := Read(fd, client.queryBuf[client.queryLen:])
	if n == 0 || err != nil {
		logger.Error().Msgf("client %v read err: %v", fd, err)
		freeClient(client)
		return
	}
	client.queryLen += n

	ProcessQueryBuf(client)
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

	resp := NewResp(queryBuf)
	for {
		value, err := resp.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			logger.Error().Msgf("read resp error: %v", err)
			return
		}

		command := value.array[0].ToStringUnsafe()
		args := value.array[1:]
		var res Value

		// lookup for command
		cmd := lookupCommand(command)
		if cmd != nil {
			res = cmd.processCommand(args)
			if server.config.AppendOnly && cmd.persist && res.typ != ERROR {
				db.aof.Write(queryBuf)
			}
		} else {
			res = newErrValue(ErrUnknownCommand(command))
		}

		client.reply = append(client.reply, res)
	}

	resetClient(client)

	// add writable event
	server.aeLoop.AddFileEvent(client.fd, AE_WRITABLE, SendReplyToClient, client)
}

func SendReplyToClient(loop *AeLoop, fd int, extra interface{}) {
	client := extra.(*Client)

	// write all replies back
	buf := make([]byte, 0, 32)
	for _, elem := range client.reply {
		buf = elem.Append(buf)
	}

	_, err := Write(fd, buf)
	if err != nil {
		logger.Error().Msgf("send reply err: %v", err)
		freeClient(client)
		return
	}

	client.reply = client.reply[:0]
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
	if err != nil {
		logger.Error().Msgf("flush aof buffer error: %v", err)
	}
}
