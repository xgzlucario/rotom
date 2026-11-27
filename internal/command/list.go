package command

import "errors"

func pushGeneric(ctx *Context, left bool) {
	ls, err := ctx.DB.ListForWrite(b2s(ctx.Args[0]))
	if err != nil {
		ctx.err(err)
		return
	}
	for _, v := range ctx.Args[1:] {
		if left {
			ls.LPush(b2s(v)) // the listpack copies the data
		} else {
			ls.RPush(b2s(v))
		}
	}
	ctx.W.WriteInt(ls.Len())
}

func lpushCommand(ctx *Context) { pushGeneric(ctx, true) }
func rpushCommand(ctx *Context) { pushGeneric(ctx, false) }

func popGeneric(ctx *Context, left bool) {
	key := b2s(ctx.Args[0])
	ls, err := ctx.DB.GetList(key)
	if err != nil {
		ctx.err(err)
		return
	}
	if ls == nil {
		ctx.unchanged()
		ctx.W.WriteNull()
		return
	}
	var v string
	if left {
		v, _ = ls.LPop()
	} else {
		v, _ = ls.RPop()
	}
	ctx.DB.DeleteIfEmpty(key, ls.Len())
	ctx.W.WriteBulkString(v)
}

func lpopCommand(ctx *Context) { popGeneric(ctx, true) }
func rpopCommand(ctx *Context) { popGeneric(ctx, false) }

func lrangeCommand(ctx *Context) {
	start, err1 := parseInt(ctx.Args[1])
	stop, err2 := parseInt(ctx.Args[2])
	if errors.Join(err1, err2) != nil {
		ctx.err(ErrParseInteger)
		return
	}
	ls, err := ctx.DB.GetList(b2s(ctx.Args[0]))
	if err != nil {
		ctx.err(err)
		return
	}
	if ls == nil {
		ctx.W.WriteArray(0)
		return
	}
	from, to, ok := normalizeRange(int(start), int(stop), ls.Len())
	if !ok {
		ctx.W.WriteArray(0)
		return
	}
	ctx.W.WriteArray(to - from + 1)
	for v := range ls.Range(from, to) {
		ctx.W.WriteBulk(v)
	}
}
