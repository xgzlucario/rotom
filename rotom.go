package main

import (
	"encoding/json"
	"fmt"
	"github.com/tidwall/redcon"
	"github.com/xgzlucario/rotom/internal/iface"
	"github.com/xgzlucario/rotom/internal/net"
	"github.com/xgzlucario/rotom/internal/resp"
	"os"

	"github.com/xgzlucario/rotom/internal/list"
)

const (
	QueryBufSize    = 8 * KB
	MaxQueryDataLen = 128 * MB
)

type (
	Map  = iface.MapI
	Set  = iface.SetI
	List = *list.QuickList
	ZSet = iface.ZSetI
)

type DB struct {
	dict *Dict
	aof  *Aof
	rdb  *Rdb
}

type Client struct {
	fd          int
	recvx       int
	readx       int
	queryBuf    []byte
	replyWriter *resp.Writer

	argsBuf [][]byte
	respBuf []redcon.RESP
}

type Server struct {
	fd      int
	config  *Config
	aeLoop  *AeLoop
	clients map[int]*Client
}

type Config struct {
	Port           int    `json:"port"`
	AppendOnly     bool   `json:"appendonly"`
	AppendFileName string `json:"appendfilename"`
	Save           bool   `json:"save"`
	SaveFileName   string `json:"savefilename"`
}

var (
	db     DB
	server Server
)

// InitDB initializes database and redo appendonly files if needed.
func InitDB(config *Config) (err error) {
	db.dict = New()

	if config.Save {
		db.rdb = NewRdb(config.SaveFileName)
		log.Debug().Msg("start loading rdb file...")
		if err = db.rdb.LoadDB(); err != nil {
			return err
		}
	}
	if config.AppendOnly {
		db.aof, err = NewAof(config.AppendFileName)
		if err != nil {
			return
		}
		log.Debug().Msg("start loading aof file...")

		// Load the initial data into memory by processing each stored command.
		emptyWriter := resp.NewWriter()
		return db.aof.Read(func(args []redcon.RESP) {
			command := b2s(args[0].Bytes())
			cmd, err := lookupCommand(command)
			if err == nil {
				cmd.process(emptyWriter, args[1:])
				emptyWriter.Reset()
			}
		})
	}
	return nil
}

func LoadConfig(path string) (config *Config, err error) {
	jsonStr, err := os.ReadFile(path)
	if err != nil {
		return
	}
	config = &Config{}
	if err = json.Unmarshal(jsonStr, config); err != nil {
		return nil, err
	}
	return
}

// AcceptHandler is the main file event of aeloop.
func AcceptHandler(loop *AeLoop, fd int, _ interface{}) {
	cfd, err := net.Accept(fd)
	if err != nil {
		log.Error().Msgf("accept err: %v", err)
		return
	}
	log.Info().Msgf("accept new client fd: %d", cfd)

	client := &Client{
		fd:          cfd,
		replyWriter: resp.NewWriter(),
		queryBuf:    make([]byte, QueryBufSize),
		argsBuf:     make([][]byte, 8),
		respBuf:     make([]redcon.RESP, 8),
	}
	server.clients[cfd] = client
	loop.AddRead(cfd, ReadQueryFromClient, client)
}

func ReadQueryFromClient(_ *AeLoop, fd int, extra interface{}) {
	client := extra.(*Client)
	readSize := 0

READ:
	n, err := net.Read(fd, client.queryBuf[client.recvx:])
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
	_ = net.Close(client.fd)
	log.Info().Msgf("free client %d", client.fd)
}

func ProcessQueryBuf(client *Client) {
	for client.readx < client.recvx {
		queryBuf := client.queryBuf[client.readx:client.recvx]
		// buffer pre alloc
		respBuf := client.respBuf[:0]
		argsBuf := client.argsBuf[:0]

		complete, args, _, left, err := redcon.ReadNextCommand(queryBuf, argsBuf)
		if err != nil {
			log.Error().Msgf("read next command error: %v", err)
			resetClient(client)
			return
		}
		if !complete {
			break
		}
		n := len(queryBuf) - len(left)
		client.readx += n

		command := b2s(args[0])
		for _, arg := range args[1:] {
			respBuf = append(respBuf, redcon.RESP{Data: arg})
		}

		cmd, err := lookupCommand(command)
		if err != nil {
			client.replyWriter.WriteError(err.Error())
			log.Error().Msg(err.Error())

		} else {
			cmd.process(client.replyWriter, respBuf)
			// write aof file
			if cmd.persist && server.config.AppendOnly {
				_, _ = db.aof.Write(queryBuf[:n])
			}
		}
	}
	if client.readx == client.recvx {
		resetClient(client)
	}
	server.aeLoop.ModWrite(client.fd, SendReplyToClient, client)
}

func SendReplyToClient(loop *AeLoop, fd int, extra interface{}) {
	client := extra.(*Client)
	sentbuf := client.replyWriter.Buffer()

	n, err := net.Write(fd, sentbuf)
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
	server.fd, err = net.TcpServer(config.Port)
	if err != nil {
		return err
	}
	return nil
}

func CronSyncAOF(_ *AeLoop, _ int, _ interface{}) {
	if err := db.aof.Flush(); err != nil {
		log.Error().Msgf("sync aof error: %v", err)
	}
}

func CronEvictExpired(_ *AeLoop, _ int, _ interface{}) {
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
