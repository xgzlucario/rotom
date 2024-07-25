package main

import (
	"fmt"
	"io"
	"runtime"
	"runtime/debug"

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

	outOfMemory bool
}

var (
	db     DB
	server Server
)

// InitDB initializes database and redo appendonly files if nedded.
func InitDB(config *Config) (err error) {
	db.dict = dict.New()

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
	if n == MAX_READER_SIZE {
		log.Error().Msgf("client %d read query too large, now free", fd)
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

		cmd := lookupCommand(command)
		if cmd != nil {
			cmd.processCommand(client.replyWriter, args)

			if server.outOfMemory {
				client.replyWriter.WriteError(errOOM)
				goto WRITE
			}

			if server.config.AppendOnly && cmd.persist {
				db.aof.Write(queryBuf)
			}

		} else {
			err := fmt.Errorf("%w '%s'", errUnknownCommand, command)
			client.replyWriter.WriteError(err)
			log.Error().Msg(err.Error())
		}
	}

WRITE:
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

func SyncAOF(loop *AeLoop, id int, extra interface{}) {
	if err := db.aof.Flush(); err != nil {
		log.Error().Msgf("sync aof error: %v", err)
	}
}

func EvictExpired(loop *AeLoop, id int, extra interface{}) {
	db.dict.EvictExpired()
}

func CheckOutOfMemory(loop *AeLoop, id int, extra interface{}) {
	oom := server.outOfMemory
	var mem runtime.MemStats

	if server.config.MaxMemory == 0 {
		if oom {
			server.outOfMemory = false
		}
		return
	}
	if oom {
		runtime.GC()
	}
	runtime.ReadMemStats(&mem)
	server.outOfMemory = int(mem.HeapAlloc) > server.config.MaxMemory
}

func SysMonitor(loop *AeLoop, id int, extra interface{}) {
	var mem runtime.MemStats
	var stat debug.GCStats

	runtime.ReadMemStats(&mem)
	debug.ReadGCStats(&stat)

	log.Info().
		Uint64("gcsys", mem.GCSys).
		Uint64("heapInuse", mem.HeapInuse).
		Uint64("heapObjects", mem.HeapObjects).
		Int64("gc", stat.NumGC).
		Msgf("[SYS]")
}
