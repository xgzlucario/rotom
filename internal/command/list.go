package command

import "errors"

func pushGeneric(ctx *Context, left bool) {
	ls, err := ctx.DB.ListForWrite(b2s(ctx.Args[0]))
	if err != nil {
		ctx.err(err)
		return
	}
	items := make([]string, 0, len(ctx.Args)-1)
	for _, arg := range ctx.Args[1:] {
		items = append(items, b2s(arg)) // the listpack copies the data
	}
	if left {
		ls.LPush(items...)
	} else {
		ls.RPush(items...)
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
	var val string
	if left {
		val, _ = ls.LPop()
	} else {
		val, _ = ls.RPop()
	}
	ctx.DB.DeleteIfEmpty(key, ls.Len())
	ctx.W.WriteBulkString(val)
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
	count := ls.RangeCount(int(start), int(stop))
	ctx.W.WriteArray(count)
	if count == 0 {
		return
	}
	ls.Range(int(start), func(data []byte) (stop bool) {
		ctx.W.WriteBulk(data)
		count--
		return count == 0
	})
}
