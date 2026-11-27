package command

import "errors"

func zaddCommand(ctx *Context) {
	pairs := ctx.Args[1:]
	if len(pairs)%2 != 0 {
		ctx.err(ErrSyntax)
		return
	}
	// validate all scores before mutating anything
	for i := 0; i < len(pairs); i += 2 {
		if _, err := parseFloat(pairs[i]); err != nil {
			ctx.err(err)
			return
		}
	}
	members := make([][]byte, 0, 8)
	for i := 1; i < len(pairs); i += 2 {
		members = append(members, pairs[i])
	}
	zs, err := ctx.DB.ZSetForWrite(b2s(ctx.Args[0]), members)
	if err != nil {
		ctx.err(err)
		return
	}
	var count int
	for i := 0; i < len(pairs); i += 2 {
		score, _ := parseFloat(pairs[i])
		if zs.Set(string(pairs[i+1]), score) { // the zset retains members: copy
			count++
		}
	}
	ctx.W.WriteInt(count)
}

func zrankCommand(ctx *Context) {
	zs, err := ctx.DB.GetZSet(b2s(ctx.Args[0]))
	if err != nil {
		ctx.err(err)
		return
	}
	rank := -1
	if zs != nil {
		rank = zs.Rank(b2s(ctx.Args[1]))
	}
	if rank < 0 {
		ctx.W.WriteNull()
	} else {
		ctx.W.WriteInt(rank)
	}
}

func zremCommand(ctx *Context) {
	key := b2s(ctx.Args[0])
	zs, err := ctx.DB.GetZSet(key)
	if err != nil {
		ctx.err(err)
		return
	}
	var count int
	if zs != nil {
		for _, m := range ctx.Args[1:] {
			if zs.Remove(b2s(m)) {
				count++
			}
		}
		ctx.DB.DeleteIfEmpty(key, zs.Len())
	}
	if count == 0 {
		ctx.unchanged()
	}
	ctx.W.WriteInt(count)
}

func zrangeCommand(ctx *Context) {
	w, args := ctx.W, ctx.Args
	start, err1 := parseInt(args[1])
	stop, err2 := parseInt(args[2])
	if errors.Join(err1, err2) != nil {
		ctx.err(ErrParseInteger)
		return
	}
	var withScores bool
	for _, arg := range args[3:] {
		if !equalFold(b2s(arg), "WITHSCORES") {
			ctx.err(ErrSyntax)
			return
		}
		withScores = true
	}
	zs, err := ctx.DB.GetZSet(b2s(args[0]))
	if err != nil {
		ctx.err(err)
		return
	}
	if zs == nil {
		w.WriteArray(0)
		return
	}
	from, to, ok := normalizeRange(int(start), int(stop), zs.Len())
	if !ok {
		w.WriteArray(0)
		return
	}
	n := to - from + 1
	if withScores {
		w.WriteArray(n * 2)
	} else {
		w.WriteArray(n)
	}
	for member, score := range zs.Range(from, to) {
		w.WriteBulkString(member)
		if withScores {
			w.WriteFloat(score)
		}
	}
}

func zpopminCommand(ctx *Context) {
	key := b2s(ctx.Args[0])
	count := 1
	if len(ctx.Args) > 2 {
		ctx.err(ErrSyntax)
		return
	}
	if len(ctx.Args) == 2 {
		n, err := parseInt(ctx.Args[1])
		if err != nil || n < 0 {
			ctx.err(ErrValueOutOfRange)
			return
		}
		count = int(n)
	}
	zs, err := ctx.DB.GetZSet(key)
	if err != nil {
		ctx.err(err)
		return
	}
	if zs == nil || count == 0 {
		ctx.unchanged()
		ctx.W.WriteArray(0)
		return
	}
	n := min(zs.Len(), count)
	ctx.W.WriteArray(n * 2)
	for range n {
		member, score := zs.PopMin()
		ctx.W.WriteBulkString(member)
		ctx.W.WriteFloat(score)
	}
	ctx.DB.DeleteIfEmpty(key, zs.Len())
}
