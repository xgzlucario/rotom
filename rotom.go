package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	cache "github.com/xgzlucario/GigaCache"
	"github.com/xgzlucario/rotom/structx"
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

type Server struct {
	config  *Config
	epoller *epoll
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
	cmdStr := b2s(ToLowerNoCopy(command))
	for _, c := range cmdTable {
		if c.name == cmdStr {
			return &c, nil
		}
	}
	return nil, fmt.Errorf("invalid command: %s", command)
}

func (cmd *Command) processCommand(args []Value) Value {
	if len(args) < cmd.arity {
		return newErrValue(ErrWrongArgs(cmd.name))
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
			log.Println("failed to initialize aof file:", err)
			return
		}

		log.Printf("start loading aof file...")

		// Load the initial data into memory by processing each stored command.
		db.aof.Read(func(value Value) {
			command := value.array[0].bulk
			args := value.array[1:]

			cmd, err := lookupCommand(command)
			if err == nil {
				cmd.processCommand(args)
			}
		})
	}

	return nil
}

func (server *Server) RunServe() {
	// Start tcp listener.
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", server.config.Port))
	if err != nil {
		log.Println("Error creating listener:", err)
		os.Exit(1)
	}
	defer listener.Close()

	// Start epoll waiter.
	epoller, err := MkEpoll()
	if err != nil {
		log.Println("Error creating epoller:", err)
		os.Exit(1)
	}
	server.epoller = epoller

	go func() {
		var buf = make([]byte, 512)

		for {
			connections, err := epoller.Wait()
			if err != nil {
				log.Println("failed to epoll wait:", err)
				continue
			}

			for _, conn := range connections {
				if conn == nil {
					break
				}

				if n, err := conn.Read(buf); err != nil {
					if err := epoller.Remove(conn); err != nil {
						log.Println("failed to remove:", err)
					}
					conn.Close()

				} else {
					server.handleConnection(buf[:n], conn)
				}
			}
		}
	}()

	log.Println("rotom server is ready to accept.")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting connection:", err)
			continue
		}
		epoller.Add(conn)
	}
}

func (server *Server) handleConnection(buf []byte, conn net.Conn) {
	resp := NewResp(bytes.NewReader(buf))
	for {
		value, err := resp.Read()
		if err != nil {
			if err != io.EOF {
				log.Println("read resp error:", err)
			}
			return
		}

		if value.typ != ARRAY || len(value.array) == 0 {
			log.Println("invalid request, expected non-empty array")
			continue
		}

		command := value.array[0].bulk
		var res Value

		cmd, err := lookupCommand(command)
		if err != nil {
			res = newErrValue(err)

		} else {
			res = cmd.processCommand(value.array[1:])

			if server.config.AppendOnly && cmd.persist && res.typ != ERROR {
				db.aof.Write(buf)
			}
		}

		if _, err = conn.Write(res.Marshal()); err != nil {
			log.Println("write reply error:", err)
		}
	}
}
