package main

import (
	"fmt"
	"io"

	"github.com/xgzlucario/rotom/internal/dict"
	"github.com/xgzlucario/rotom/internal/hash"
	"github.com/xgzlucario/rotom/internal/list"
	"github.com/xgzlucario/rotom/internal/zset"
	lua "github.com/yuin/gopher-lua"
)

const (
	QueryBufSize = 8 * KB
	WriteBufSize = 8 * KB

	MaxQueryDataLen = 128 * MB
)

type (
	Map  = hash.MapI
	Set  = hash.SetI
	List = *list.QuickList
	ZSet = *zset.ZSet
)

type DB struct {
	dict *dict.Dict
	aof  *Aof
}

type Client struct {
	fd          int
	recvx       int
	readx       int
	queryBuf    []byte
	argsBuf     []RESP
	replyWriter *RESPWriter
}

type Server struct {
	fd          int
	config      *Config
	aeLoop      *AeLoop
	clients     map[int]*Client
	lua         *lua.LState
	outOfMemory bool
}

var (
	db     DB
	server Server
)

// InitDB initializes database and redo appendonly files if needed.
func InitDB(config *Config) (err error) {
	db.dict = dict.New()

	if config.AppendOnly {
		db.aof, err = NewAof(config.AppendFileName)
		if err != nil {
			return
		}

		log.Debug().Msg("start loading aof file...")

		// Load the initial data into memory by processing each stored command.
		emptyWriter := NewWriter(WriteBufSize)
		return db.aof.Read(func(args []RESP) {
			command := args[0].ToStringUnsafe()

			cmd, err := lookupCommand(command)
			if err == nil {
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
		replyWriter: NewWriter(WriteBufSize),
		queryBuf:    make([]byte, QueryBufSize),
		argsBuf:     make([]RESP, 8),
	}

	server.clients[cfd] = client
	loop.AddRead(cfd, ReadQueryFromClient, client)
}

func ReadQueryFromClient(_ *AeLoop, fd int, extra interface{}) {
	client := extra.(*Client)
	readSize := 0

READ:
	n, err := Read(fd, client.queryBuf[client.recvx:])
	if err != nil {
		log.Error().Msgf("client %v read err: %v", fd, err)
		freeClient(client)
		return
	}
	readSize += n
	client.recvx += n

	if readSize == 0 {
		freeClient(client)
		return
	}

	if client.recvx >= MaxQueryDataLen {
		log.Error().Msgf("client %d read query data too large, now free", fd)
		freeClient(client)
		return
	}

	// queryBuf need grow up
	if client.recvx == len(client.queryBuf) {
		client.queryBuf = append(client.queryBuf, make([]byte, client.recvx)...)
		log.Warn().Msgf("client %d queryBuf grow up to size %s", fd, readableSize(len(client.queryBuf)))
		goto READ
	}

	ProcessQueryBuf(client)
}

func resetClient(client *Client) {
	client.readx = 0
	client.recvx = 0
}

func freeClient(client *Client) {
	delete(server.clients, client.fd)
	server.aeLoop.ModDetach(client.fd)
	Close(client.fd)
}

func ProcessQueryBuf(client *Client) {
	queryBuf := client.queryBuf[client.readx:client.recvx]

	reader := NewReader(queryBuf)
	for {
		args, n, err := reader.ReadNextCommand(client.argsBuf)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Error().Msgf("read resp error: %v", err)
			return
		}
		client.readx += n

		command := args[0].ToStringUnsafe()
		args = args[1:]

		cmd, err := lookupCommand(command)
		if err != nil {
			client.replyWriter.WriteError(err)
			log.Error().Msg(err.Error())

		} else {
			// reject write request when OOM
			if cmd.persist && server.outOfMemory {
				client.replyWriter.WriteError(errOOM)
				goto WRITE
			}

			cmd.processCommand(client.replyWriter, args)

			// write aof file
			if cmd.persist && server.config.AppendOnly {
				db.aof.Write(queryBuf)
			}
		}
	}

WRITE:
	resetClient(client)
	server.aeLoop.ModWrite(client.fd, SendReplyToClient, client)
}

func SendReplyToClient(loop *AeLoop, fd int, extra interface{}) {
	client := extra.(*Client)
	sentbuf := client.replyWriter.b

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
	// init aeLoop
	server.aeLoop, err = AeLoopCreate()
	if err != nil {
		return err
	}
	// init tcp server
	server.fd, err = TcpServer(config.Port)
	if err != nil {
		_ = Close(server.fd)
		return err
	}
	// init lua state
	L := lua.NewState()
	L.Push(L.NewFunction(OpenRedis))
	L.Push(lua.LString("redis"))
	L.Call(1, 0)
	server.lua = L

	return nil
}

func CronSyncAOF(loop *AeLoop, id int, extra interface{}) {
	if err := db.aof.Flush(); err != nil {
		log.Error().Msgf("sync aof error: %v", err)
	}
}

func CronEvictExpired(loop *AeLoop, id int, extra interface{}) {
	db.dict.EvictExpired()
}

func readableSize[T int | uint64](sz T) string {
	switch {
	case sz >= GB:
		return fmt.Sprintf("%.1fGB", float64(sz)/float64(GB))
	case sz >= MB:
		return fmt.Sprintf("%.1fMB", float64(sz)/float64(MB))
	case sz >= KB:
		return fmt.Sprintf("%.1fKB", float64(sz)/float64(KB))
	}
	return fmt.Sprintf("%dB", sz)
}
