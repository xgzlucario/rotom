package command

func saddCommand(ctx *Context) {
	members := ctx.Args[1:]
	set, err := ctx.DB.SetForWrite(b2s(ctx.Args[0]), len(members), maxLen(members))
	if err != nil {
		ctx.err(err)
		return
	}
	var count int
	for _, m := range members {
		if set.Add(string(m)) {
			count++
		}
	}
	if count == 0 {
		ctx.unchanged()
	}
	ctx.W.WriteInt(count)
}

func sremCommand(ctx *Context) {
	key := b2s(ctx.Args[0])
	set, err := ctx.DB.GetSet(key)
	if err != nil {
		ctx.err(err)
		return
	}
	var count int
	if set != nil {
		for _, m := range ctx.Args[1:] {
			if set.Remove(b2s(m)) {
				count++
			}
		}
		ctx.DB.DeleteIfEmpty(key, set.Len())
	}
	if count == 0 {
		ctx.unchanged()
	}
	ctx.W.WriteInt(count)
}

func smembersCommand(ctx *Context) {
	set, err := ctx.DB.GetSet(b2s(ctx.Args[0]))
	if err != nil {
		ctx.err(err)
		return
	}
	if set == nil {
		ctx.W.WriteArray(0)
		return
	}
	ctx.W.WriteArray(set.Len())
	set.Scan(func(m string) { ctx.W.WriteBulkString(m) })
}

func spopCommand(ctx *Context) {
	key := b2s(ctx.Args[0])
	set, err := ctx.DB.GetSet(key)
	if err != nil {
		ctx.err(err)
		return
	}
	if set == nil {
		ctx.unchanged()
		ctx.W.WriteNull()
		return
	}
	member, _ := set.Pop()
	ctx.DB.DeleteIfEmpty(key, set.Len())
	// SPOP is not deterministic: persist which member was removed.
	ctx.rewrite([]byte("srem"), ctx.Args[0], []byte(member))
	ctx.W.WriteBulkString(member)
}
