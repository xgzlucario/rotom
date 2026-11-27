package command

import "fmt"

func hsetCommand(ctx *Context) {
	args := ctx.Args
	pairs := args[1:]
	if len(pairs)%2 == 1 {
		ctx.W.WriteError(fmt.Sprintf("%s for 'hset' command", ErrWrongArguments))
		return
	}
	hmap, err := ctx.DB.HashForWrite(b2s(args[0]))
	if err != nil {
		ctx.err(err)
		return
	}
	var count int
	for i := 0; i < len(pairs); i += 2 {
		// field is retained by the map, value is copied into its buffer.
		if hmap.Set(string(pairs[i]), pairs[i+1]) {
			count++
		}
	}
	ctx.W.WriteInt(count)
}

func hgetCommand(ctx *Context) {
	hmap, err := ctx.DB.GetHash(b2s(ctx.Args[0]))
	if err != nil {
		ctx.err(err)
		return
	}
	if hmap != nil {
		if value, ok := hmap.Get(b2s(ctx.Args[1])); ok {
			ctx.W.WriteBulk(value)
			return
		}
	}
	ctx.W.WriteNull()
}

func hdelCommand(ctx *Context) {
	key := b2s(ctx.Args[0])
	hmap, err := ctx.DB.GetHash(key)
	if err != nil {
		ctx.err(err)
		return
	}
	var count int
	if hmap != nil {
		for _, field := range ctx.Args[1:] {
			if hmap.Remove(b2s(field)) {
				count++
			}
		}
		ctx.DB.DeleteIfEmpty(key, hmap.Len())
	}
	if count == 0 {
		ctx.unchanged()
	}
	ctx.W.WriteInt(count)
}

func hgetallCommand(ctx *Context) {
	hmap, err := ctx.DB.GetHash(b2s(ctx.Args[0]))
	if err != nil {
		ctx.err(err)
		return
	}
	if hmap == nil {
		ctx.W.WriteArray(0)
		return
	}
	ctx.W.WriteArray(hmap.Len() * 2)
	hmap.Scan(func(key string, value []byte) {
		ctx.W.WriteBulkString(key)
		ctx.W.WriteBulk(value)
	})
}
