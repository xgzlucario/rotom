package command

import (
	"math"
	"strconv"
	"strings"
)

func setCommand(ctx *Context) {
	args := ctx.Args
	key := b2s(args[0])
	var expireAt int64 // unix ms, 0 means no expire
	var keepTTL, nx, xx, hasExpire bool

	for extra := args[2:]; len(extra) > 0; {
		arg := b2s(extra[0])
		switch {
		case isExpireOption(arg) && len(extra) >= 2 && !hasExpire && !keepTTL:
			n, err := parseInt(extra[1])
			if err != nil {
				ctx.err(err)
				return
			}
			at, ok := toExpireAt(strings.ToUpper(arg), n, ctx.DB.Now())
			if n <= 0 || !ok {
				ctx.err(errInvalidExpire("set"))
				return
			}
			expireAt, hasExpire = at, true
			extra = extra[2:]
		case equalFold(arg, "KEEPTTL") && !hasExpire:
			keepTTL = true
			extra = extra[1:]
		case equalFold(arg, "NX") && !xx:
			nx = true
			extra = extra[1:]
		case equalFold(arg, "XX") && !nx:
			xx = true
			extra = extra[1:]
		default:
			ctx.err(ErrSyntax)
			return
		}
	}

	if nx || xx {
		if exists := ctx.DB.Exists(key); (nx && exists) || (xx && !exists) {
			ctx.unchanged()
			ctx.W.WriteNull()
			return
		}
	}
	ctx.DB.SetString(key, args[1], expireAt, keepTTL)
	if hasExpire {
		// persist an absolute time, so that replaying the AOF does not extend the TTL.
		ctx.rewrite([]byte("set"), args[0], args[1], []byte("pxat"), strconv.AppendInt(nil, expireAt, 10))
	}
	ctx.W.WriteString("OK")
}

func getCommand(ctx *Context) {
	v, ok, err := ctx.DB.GetString(b2s(ctx.Args[0]))
	switch {
	case err != nil:
		ctx.err(err)
	case !ok:
		ctx.W.WriteNull()
	default:
		ctx.W.WriteBulk(v)
	}
}

func incrCommand(ctx *Context) {
	key := b2s(ctx.Args[0])
	v, ok, err := ctx.DB.GetString(key)
	if err != nil {
		ctx.err(err)
		return
	}
	var num int64
	if ok {
		if num, err = strconv.ParseInt(b2s(v), 10, 64); err != nil {
			ctx.err(ErrParseInteger)
			return
		}
	}
	if num == math.MaxInt64 {
		ctx.err(ErrOverflow)
		return
	}
	num++
	var buf [20]byte
	ctx.DB.SetString(key, strconv.AppendInt(buf[:0], num, 10), 0, true) // INCR keeps the TTL
	ctx.W.WriteInt64(num)
}
