package command

func saddCommand(ctx *Context) {
	members := ctx.Args[1:]
	s, err := ctx.DB.SetForWrite(b2s(ctx.Args[0]), members)
	if err != nil {
		ctx.err(err)
		return
	}
	var count int
	for _, m := range members {
		if s.Add(string(m)) { // the set retains members: copy
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
	s, err := ctx.DB.GetSet(key)
	if err != nil {
		ctx.err(err)
		return
	}
	var count int
	if s != nil {
		for _, m := range ctx.Args[1:] {
			if s.Remove(b2s(m)) {
				count++
			}
		}
		ctx.DB.DeleteIfEmpty(key, s.Len())
	}
	if count == 0 {
		ctx.unchanged()
	}
	ctx.W.WriteInt(count)
}

func smembersCommand(ctx *Context) {
	s, err := ctx.DB.GetSet(b2s(ctx.Args[0]))
	if err != nil {
		ctx.err(err)
		return
	}
	if s == nil {
		ctx.W.WriteArray(0)
		return
	}
	ctx.W.WriteArray(s.Len())
	for m := range s.All() {
		ctx.W.WriteBulkString(m)
	}
}

func spopCommand(ctx *Context) {
	key := b2s(ctx.Args[0])
	s, err := ctx.DB.GetSet(key)
	if err != nil {
		ctx.err(err)
		return
	}
	if s == nil {
		ctx.unchanged()
		ctx.W.WriteNull()
		return
	}
	member, _ := s.Pop()
	ctx.DB.DeleteIfEmpty(key, s.Len())
	// SPOP is not deterministic: persist which member was removed.
	ctx.rewrite([]byte("srem"), ctx.Args[0], []byte(member))
	ctx.W.WriteBulkString(member)
}
