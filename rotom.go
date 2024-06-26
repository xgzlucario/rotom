package main

import (
	"io"

	"github.com/cockroachdb/swiss"
	"github.com/xgzlucario/rotom/internal/dict"
	"github.com/xgzlucario/rotom/internal/hash"
	"github.com/xgzlucario/rotom/internal/list"
	"github.com/xgzlucario/rotom/internal/zset"
)

const (
	READ_BUF_SIZE   = 16 * KB
	WRITE_BUF_SIZE  = 4 * KB
	MAX_READER_SIZE = 4 * KB
)

type (
	Map  = *hash.Map
	Set  = *hash.Set
	List = *list.QuickList
	ZSet = *zset.ZSet
)

type DB struct {
	strs   *dict.Dict
	extras *swiss.Map[string, any]
	aof    *Aof
}

type Client struct {
	fd          int
	queryLen    int
	queryBuf    []byte
	argsBuf     []RESP
	replyWriter *RESPWriter
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
	db.strs = dict.New(dict.DefaultOptions)
	db.extras = swiss.New[string, any](64)

	if config.AppendOnly {
		db.aof, err = NewAof(config.AppendFileName)
		if err != nil {
			return
		}

		log.Debug().Msg("start loading aof file...")

		// Load the initial data into memory by processing each stored command.
		emptyWriter := NewWriter(WRITE_BUF_SIZE)
		return db.aof.Read(func(args []RESP) {
			command := args[0].ToString()

			cmd := lookupCommand(command)
			if cmd != nil {
				cmd.processCommand(emptyWriter, args[1:])
				emptyWriter.Reset()
			}
		})
	}

	return nil
}

// AcceptHandler is the main file event of aeloop.
func AcceptHandler(loop *AeLoop, fd int, _ interface{}) {
	cfd, err := Accept(fd)
	if err != nil {
		log.Error().Msgf("accept err: %v", err)
		return
	}
	// create client
	client := &Client{
		fd:          cfd,
		replyWriter: NewWriter(WRITE_BUF_SIZE),
		queryBuf:    make([]byte, READ_BUF_SIZE),
		argsBuf:     make([]RESP, 8),
	}

	log.Debug().Msgf("accept client, fd: %d", cfd)
	server.clients[cfd] = client
	loop.AddRead(cfd, ReadQueryFromClient, client)
}

func ReadQueryFromClient(loop *AeLoop, fd int, extra interface{}) {
	client := extra.(*Client)

	// grow query buffer
	if len(client.queryBuf)-client.queryLen < MAX_READER_SIZE {
		client.queryBuf = append(client.queryBuf, make([]byte, MAX_READER_SIZE)...)
	}

	n, err := Read(fd, client.queryBuf[client.queryLen:])
	if err != nil {
		log.Error().Msgf("client %v read err: %v", fd, err)
		freeClient(client)
		return
	}
	if n == 0 {
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
	server.aeLoop.ModDetach(client.fd)
	Close(client.fd)
}

func ProcessQueryBuf(client *Client) {
	queryBuf := client.queryBuf[:client.queryLen]

	reader := NewReader(queryBuf)
	for {
		args, err := reader.ReadNextCommand(client.argsBuf)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Error().Msgf("read resp error: %v", err)
			return
		}

		command := args[0].ToStringUnsafe()
		args = args[1:]

		// lookup for command
		cmd := lookupCommand(command)
		if cmd != nil {
			cmd.processCommand(client.replyWriter, args)
			if server.config.AppendOnly && cmd.persist { // TODO: optimize AOF operation
				db.aof.Write(queryBuf)
			}
		} else {
			err := ErrUnknownCommand(command)
			client.replyWriter.WriteError(err)
			log.Warn().Msgf("ERR %v", err)
		}
	}

	resetClient(client)
	server.aeLoop.ModWrite(client.fd, SendReplyToClient, client)
}

func SendReplyToClient(loop *AeLoop, fd int, extra interface{}) {
	client := extra.(*Client)
	sentbuf := client.replyWriter.b.Bytes()

	n, err := Write(fd, sentbuf)
	if err != nil {
		log.Error().Msgf("send reply err: %v", err)
		freeClient(client)
		return
	}
	if n != len(sentbuf) {
		log.Error().Msgf("send packet size error: %d %d", n, len(sentbuf))
	}

	client.replyWriter.Reset()
	loop.ModRead(fd, ReadQueryFromClient, client)
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

func ServerCronFlush(loop *AeLoop, id int, extra interface{}) {
	if db.aof == nil {
		return
	}
	err := db.aof.Flush()
	if err != nil {
		log.Error().Msgf("flush aof buffer error: %v", err)
	}
}

func ServerCronEvict(loop *AeLoop, id int, extra interface{}) {
	db.strs.EvictExpired()
}
