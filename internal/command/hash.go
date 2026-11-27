package command

import "fmt"

func hsetCommand(ctx *Context) {
	pairs := ctx.Args[1:]
	if len(pairs)%2 == 1 {
		ctx.W.WriteError(fmt.Sprintf("%s for 'hset' command", ErrWrongArguments))
		return
	}
	h, err := ctx.DB.HashForWrite(b2s(ctx.Args[0]))
	if err != nil {
		ctx.err(err)
		return
	}
	var count int
	for i := 0; i < len(pairs); i += 2 {
		if h.Set(b2s(pairs[i]), pairs[i+1]) { // field and value are copied
			count++
		}
	}
	ctx.W.WriteInt(count)
}

func hgetCommand(ctx *Context) {
	h, err := ctx.DB.GetHash(b2s(ctx.Args[0]))
	if err != nil {
		ctx.err(err)
		return
	}
	if h != nil {
		if v, ok := h.Get(b2s(ctx.Args[1])); ok {
			ctx.W.WriteBulk(v)
			return
		}
	}
	ctx.W.WriteNull()
}

func hdelCommand(ctx *Context) {
	key := b2s(ctx.Args[0])
	h, err := ctx.DB.GetHash(key)
	if err != nil {
		ctx.err(err)
		return
	}
	var count int
	if h != nil {
		for _, field := range ctx.Args[1:] {
			if h.Remove(b2s(field)) {
				count++
			}
		}
		ctx.DB.DeleteIfEmpty(key, h.Len())
	}
	if count == 0 {
		ctx.unchanged()
	}
	ctx.W.WriteInt(count)
}

func hgetallCommand(ctx *Context) {
	h, err := ctx.DB.GetHash(b2s(ctx.Args[0]))
	if err != nil {
		ctx.err(err)
		return
	}
	if h == nil {
		ctx.W.WriteArray(0)
		return
	}
	ctx.W.WriteArray(h.Len() * 2)
	for field, value := range h.All() {
		ctx.W.WriteBulkString(field)
		ctx.W.WriteBulk(value)
	}
}
