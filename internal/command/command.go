// Package command implements the rotom command table and handlers.
package command

import (
	"fmt"
	"runtime/debug"

	"github.com/rs/zerolog/log"
	"github.com/xgzlucario/rotom/internal/config"
	"github.com/xgzlucario/rotom/internal/db"
	"github.com/xgzlucario/rotom/internal/resp"
)

type Flags uint8

const (
	// FlagWrite marks commands that may modify the dataset. They are
	// appended to the AOF when they succeed and change something.
	FlagWrite Flags = 1 << iota
)

type Command struct {
	Name    string
	handler func(*Context)
	// arity follows redis: it counts the command name; a negative value
	// -N means "at least N".
	arity int
	flags Flags
}

func (c *Command) IsWrite() bool { return c.flags&FlagWrite != 0 }

func (c *Command) checkArity(argc int) bool {
	if c.arity >= 0 {
		return argc == c.arity
	}
	return argc >= -c.arity
}

// Context carries one command invocation. A server keeps one Context per
// client and reuses it for every command, so executing does not allocate.
type Context struct {
	W      *resp.Writer
	Args   [][]byte // arguments, excluding the command name
	DB     *db.DB
	Config *config.Config

	// propagate, when set, is written to the AOF instead of the original
	// command (e.g. relative TTLs rewritten to absolute times).
	propagate [][]byte
	// noPropagate skips the AOF for a write command that changed nothing.
	noPropagate bool
}

// rewrite replaces what is persisted to the AOF for this command.
func (ctx *Context) rewrite(argv ...[]byte) { ctx.propagate = argv }

// unchanged marks that a write command did not modify the dataset.
func (ctx *Context) unchanged() { ctx.noPropagate = true }

func (ctx *Context) err(err error) { ctx.W.WriteError(err.Error()) }

var commands = map[string]*Command{}

func register(name string, handler func(*Context), arity int, flags Flags) {
	commands[name] = &Command{Name: name, handler: handler, arity: arity, flags: flags}
}

func init() {
	const w = FlagWrite
	// keys
	register("del", delCommand, -2, w)
	register("exists", existsCommand, -2, 0)
	register("type", typeCommand, 2, 0)
	register("scan", scanCommand, -2, 0)
	register("expire", expireCommand, 3, w)
	register("pexpire", pexpireCommand, 3, w)
	register("expireat", expireatCommand, 3, w)
	register("pexpireat", pexpireatCommand, 3, w)
	register("ttl", ttlCommand, 2, 0)
	register("pttl", pttlCommand, 2, 0)
	register("persist", persistCommand, 2, w)
	// strings
	register("set", setCommand, -3, w)
	register("get", getCommand, 2, 0)
	register("incr", incrCommand, 2, w)
	// hashes
	register("hset", hsetCommand, -4, w)
	register("hget", hgetCommand, 3, 0)
	register("hdel", hdelCommand, -3, w)
	register("hgetall", hgetallCommand, 2, 0)
	// lists
	register("lpush", lpushCommand, -3, w)
	register("rpush", rpushCommand, -3, w)
	register("lpop", lpopCommand, 2, w)
	register("rpop", rpopCommand, 2, w)
	register("lrange", lrangeCommand, 4, 0)
	// sets
	register("sadd", saddCommand, -3, w)
	register("srem", sremCommand, -3, w)
	register("spop", spopCommand, 2, w)
	register("smembers", smembersCommand, 2, 0)
	// sorted sets
	register("zadd", zaddCommand, -4, w)
	register("zrem", zremCommand, -3, w)
	register("zrank", zrankCommand, 3, 0)
	register("zpopmin", zpopminCommand, -2, w)
	register("zrange", zrangeCommand, -4, 0)
	// server
	register("ping", pingCommand, -1, 0)
	register("hello", helloCommand, -1, 0)
	register("flushdb", flushdbCommand, -1, w)
	register("save", saveCommand, 1, 0)
	register("load", loadCommand, 1, 0)
	register("config", configCommand, -2, 0)
}

const maxNameLen = 32

// Lookup finds a command by case-insensitive name without allocating.
func Lookup(name []byte) *Command {
	if len(name) > maxNameLen {
		return nil
	}
	var buf [maxNameLen]byte
	for i, c := range name {
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		buf[i] = c
	}
	return commands[string(buf[:len(name)])]
}

// Result tells the caller how to persist an executed command.
type Result struct {
	// Persist is true if the command changed the dataset.
	Persist bool
	// Argv, when non-nil, replaces the original command in the AOF.
	Argv [][]byte
}

// Execute runs cmd with args (excluding the command name), appending the
// reply to ctx.W. A panic in the handler is turned into an error reply, so a
// bug in one command cannot take down the whole server.
func Execute(cmd *Command, ctx *Context, args [][]byte) (res Result) {
	ctx.Args, ctx.propagate, ctx.noPropagate = args, nil, false
	w := ctx.W
	if !cmd.checkArity(len(ctx.Args) + 1) {
		w.WriteError(fmt.Sprintf("%s for '%s' command", ErrWrongArguments, cmd.Name))
		return
	}
	ctx.DB.Tick()
	mark := w.Len()
	defer func() {
		if r := recover(); r != nil {
			w.Truncate(mark)
			w.WriteError(fmt.Sprintf("ERR internal error in '%s': %v", cmd.Name, r))
			log.Error().Msgf("panic in command '%s': %v\n%s", cmd.Name, r, debug.Stack())
			res = Result{}
		}
	}()
	cmd.handler(ctx)

	if !cmd.IsWrite() || ctx.noPropagate || resp.IsError(w.Bytes()[mark:]) {
		return Result{}
	}
	return Result{Persist: true, Argv: ctx.propagate}
}
