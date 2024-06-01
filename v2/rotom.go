package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"os"

	cache "github.com/xgzlucario/GigaCache"
	"github.com/xgzlucario/rotom/structx"
)

// Type aliases for built-in types.
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

type Server struct{}

type Command struct {
	name    string
	handler func([]Value) Value
	arity   int // arity represents the minimal number of arguments that command accepts.
	aofNeed bool
}

// global varibles
var (
	config   *Config
	db       DB
	server   Server
	cmdTable []Command = []Command{
		{"ping", pingCommand, 0, false},
		{"set", setCommand, 2, true},
		{"get", getCommand, 1, false},
		{"hset", hsetCommand, 3, true},
		{"hget", hgetCommand, 2, false},
		{"hdel", hdelCommand, 2, true},
		{"hgetall", hgetallCommand, 1, false},
		// TODO
	}
)

func lookupCommand(command string) (*Command, error) {
	for _, c := range cmdTable {
		if c.name == command {
			return &c, nil
		}
	}
	return nil, fmt.Errorf("invalid command: %s", command)
}

func (cmd *Command) writeAofFile(aof *Aof, args []Value) {
	if cmd.aofNeed {
		aof.Write(Value{typ: ARRAY, array: args})
	}
}

func (cmd *Command) processCommand(args []Value) Value {
	// Check command args.
	if len(args) < cmd.arity {
		return NewErrValue(ErrWrongArgs(cmd.name))
	}
	return cmd.handler(args)
}

func InitDB() (err error) {
	options := cache.DefaultOptions
	options.ConcurrencySafe = false
	db.strs = cache.New(options)
	db.extras = make(map[string]any)

	if config.AppendOnly {
		db.aof, err = NewAof(config.AppendFileName)
		if err != nil {
			log.Printf("failed to initialize aof file: %v\n", err)
			return
		}

		log.Printf("start loading aof file...")

		// Load the initial data into memory by processing each stored command.
		db.aof.Read(func(value Value) {
			command := bytes.ToLower(value.array[0].bulk)
			args := value.array[1:]

			cmd, err := lookupCommand(b2s(command))
			if err == nil {
				cmd.processCommand(args)
			}
		})
	}

	return nil
}

func (s *Server) Run() {
	epoller, err := MkEpoll()
	if err != nil {
		panic(err)
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Port))
	if err != nil {
		fmt.Println("Error creating listener:", err)
		os.Exit(1)
	}
	defer listener.Close()

	// epoll waiter
	go func() {
		var buf = make([]byte, 512)
		for {
			connections, err := epoller.Wait()
			if err != nil {
				log.Printf("failed to epoll wait %v", err)
				continue
			}

			for _, conn := range connections {
				if conn == nil {
					break
				}

				if n, err := conn.Read(buf); err != nil {
					if err := epoller.Remove(conn); err != nil {
						log.Printf("failed to remove %v", err)
					}
					conn.Close()

				} else {
					handleConnection(buf[:n], conn)
				}
			}
		}
	}()

	log.Println("rotom server is ready to accept.")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		epoller.Add(conn)
	}
}

func ErrWrongArgs(cmd string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd)
}
