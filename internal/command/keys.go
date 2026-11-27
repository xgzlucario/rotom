package command

import (
	"math"
	"strconv"
	"strings"

	"github.com/tidwall/match"

	"github.com/xgzlucario/rotom/internal/db"
)

func delCommand(ctx *Context) {
	var count int
	for _, arg := range ctx.Args {
		if ctx.DB.Delete(b2s(arg)) {
			count++
		}
	}
	if count == 0 {
		ctx.unchanged()
	}
	ctx.W.WriteInt(count)
}

func existsCommand(ctx *Context) {
	var count int
	for _, arg := range ctx.Args {
		if _, ok := ctx.DB.Get(b2s(arg)); ok {
			count++
		}
	}
	ctx.W.WriteInt(count)
}

func typeCommand(ctx *Context) {
	v, _ := ctx.DB.Get(b2s(ctx.Args[0]))
	ctx.W.WriteString(typeName(v))
}

// scanCommand implements SCAN cursor [MATCH pattern] [COUNT count] [TYPE type].
func scanCommand(ctx *Context) {
	w, args := ctx.W, ctx.Args
	cursor, err := strconv.ParseUint(b2s(args[0]), 10, 64)
	if err != nil {
		ctx.err(ErrInvalidCursor)
		return
	}
	count := 10
	var pattern, typ string
	for extra := args[1:]; len(extra) > 0; extra = extra[2:] {
		if len(extra) < 2 {
			ctx.err(ErrSyntax)
			return
		}
		opt, val := b2s(extra[0]), b2s(extra[1])
		switch {
		case equalFold(opt, "COUNT"):
			n, err := parseInt(extra[1])
			if err != nil {
				ctx.err(err)
				return
			}
			if n < 1 {
				ctx.err(ErrSyntax)
				return
			}
			count = int(n)
		case equalFold(opt, "MATCH"):
			pattern = val
		case equalFold(opt, "TYPE"):
			typ = strings.ToLower(val)
		default:
			ctx.err(ErrSyntax)
			return
		}
	}

	// Like redis, COUNT is a hint: visit buckets until enough keys were
	// collected, bounded so a sparse table cannot block the server.
	keys := make([]string, 0, count)
	for visits := 0; visits < count*10; visits++ {
		cursor = ctx.DB.Scan(cursor, func(key string, v any) {
			if pattern != "" && !match.Match(key, pattern) {
				return
			}
			if typ != "" && typeName(v) != typ {
				return
			}
			keys = append(keys, key)
		})
		if cursor == 0 || len(keys) >= count {
			break
		}
	}
	w.WriteArray(2)
	w.WriteBulkString(strconv.FormatUint(cursor, 10))
	w.WriteBulkStrings(keys)
}

func isExpireOption(s string) bool {
	return equalFold(s, "EX") || equalFold(s, "PX") || equalFold(s, "EXAT") || equalFold(s, "PXAT")
}

// toExpireAt converts an expire argument to an absolute unix ms time.
// unit is one of EX, PX, EXAT, PXAT. ok is false on overflow.
func toExpireAt(unit string, n, now int64) (int64, bool) {
	var at int64
	switch unit {
	case "EX", "EXAT":
		if n > math.MaxInt64/1000 || n < math.MinInt64/1000 {
			return 0, false
		}
		at = n * 1000
	default:
		at = n
	}
	if unit == "EX" || unit == "PX" {
		if at > math.MaxInt64-now {
			return 0, false
		}
		at += now
	}
	return at, true
}

func expireGeneric(ctx *Context, cmd, unit string) {
	args := ctx.Args
	n, err := parseInt(args[1])
	if err != nil {
		ctx.err(err)
		return
	}
	at, ok := toExpireAt(unit, n, ctx.DB.Now())
	if !ok {
		ctx.err(errInvalidExpire(cmd))
		return
	}
	if !ctx.DB.SetExpireAt(b2s(args[0]), at) {
		ctx.unchanged()
		ctx.W.WriteInt(0)
		return
	}
	// persist an absolute time, so that replaying the AOF does not extend the TTL.
	if at <= ctx.DB.Now() {
		ctx.rewrite([]byte("del"), args[0])
	} else {
		ctx.rewrite([]byte("pexpireat"), args[0], strconv.AppendInt(nil, at, 10))
	}
	ctx.W.WriteInt(1)
}

func expireCommand(ctx *Context)    { expireGeneric(ctx, "expire", "EX") }
func pexpireCommand(ctx *Context)   { expireGeneric(ctx, "pexpire", "PX") }
func expireatCommand(ctx *Context)  { expireGeneric(ctx, "expireat", "EXAT") }
func pexpireatCommand(ctx *Context) { expireGeneric(ctx, "pexpireat", "PXAT") }

func ttlGeneric(ctx *Context, ms bool) {
	at, ok := ctx.DB.ExpireAt(b2s(ctx.Args[0]))
	switch {
	case !ok:
		ctx.W.WriteInt(-2)
	case at == 0:
		ctx.W.WriteInt(-1)
	case ms:
		ctx.W.WriteInt64(at - ctx.DB.Now())
	default:
		// round to the nearest second, like redis
		ctx.W.WriteInt64((at - ctx.DB.Now() + 500) / 1000)
	}
}

func ttlCommand(ctx *Context)  { ttlGeneric(ctx, false) }
func pttlCommand(ctx *Context) { ttlGeneric(ctx, true) }

func persistCommand(ctx *Context) {
	if ctx.DB.Persist(b2s(ctx.Args[0])) {
		ctx.W.WriteInt(1)
	} else {
		ctx.unchanged()
		ctx.W.WriteInt(0)
	}
}

func typeName(v any) string { return db.TypeOf(v).String() }
