package command

import (
	"fmt"
	"strings"

	"github.com/xgzlucario/rotom/internal/db"
)

func pingCommand(ctx *Context) {
	switch len(ctx.Args) {
	case 0:
		ctx.W.WriteString("PONG")
	case 1:
		ctx.W.WriteBulk(ctx.Args[0])
	default:
		ctx.W.WriteError(fmt.Sprintf("%s for 'ping' command", ErrWrongArguments))
	}
}

func helloCommand(ctx *Context) {
	w := ctx.W
	// RESP2 encodes maps as flat arrays.
	w.WriteArray(10)
	w.WriteBulkString("server")
	w.WriteBulkString("rotom")
	w.WriteBulkString("version")
	w.WriteBulkString("1.0.0")
	w.WriteBulkString("proto")
	w.WriteInt(2)
	w.WriteBulkString("mode")
	w.WriteBulkString("standalone")
	w.WriteBulkString("role")
	w.WriteBulkString("master")
}

func flushdbCommand(ctx *Context) {
	ctx.DB.Dict = db.NewDict()
	ctx.W.WriteString("OK")
}

func saveCommand(ctx *Context) {
	if err := db.SaveRDB(ctx.Config.RDB.DBFileName, ctx.DB.Dict); err != nil {
		ctx.W.WriteError("ERR " + err.Error())
		return
	}
	ctx.W.WriteString("OK")
}

func loadCommand(ctx *Context) {
	// load into a new dict and swap only on success.
	dict, err := db.LoadRDB(ctx.Config.RDB.DBFileName)
	if err != nil {
		ctx.W.WriteError("ERR " + err.Error())
		return
	}
	ctx.DB.Dict = dict
	ctx.W.WriteString("OK")
}

// configCommand implements CONFIG GET <name>. CONFIG SET is not supported.
func configCommand(ctx *Context) {
	w, args := ctx.W, ctx.Args
	op := b2s(args[0])
	switch {
	case equalFold(op, "get") && len(args) == 2:
		name := strings.ToLower(b2s(args[1]))
		value, ok := ctx.Config.Get(name)
		if !ok {
			w.WriteArray(0)
			return
		}
		w.WriteArray(2)
		w.WriteBulkString(name)
		w.WriteBulkString(value)
	case equalFold(op, "set"):
		w.WriteError("ERR CONFIG SET is not supported")
	default:
		w.WriteError(fmt.Sprintf("ERR unknown subcommand or wrong number of arguments for '%s'", op))
	}
}
