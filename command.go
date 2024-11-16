package main

import (
	"fmt"
	"github.com/xgzlucario/rotom/internal/resp"
	"github.com/xgzlucario/rotom/internal/timer"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/xgzlucario/rotom/internal/hash"
	"github.com/xgzlucario/rotom/internal/list"
	"github.com/xgzlucario/rotom/internal/zset"
	lua "github.com/yuin/gopher-lua"
)

var (
	WithScores = "WITHSCORES"
	KeepTtl    = "KEEPTTL"
	NX         = "NX"
	EX         = "EX"
	PX         = "PX"
)

type Command struct {
	// name is lowercase letters command name.
	name string

	// handler is this command real database handler function.
	handler func(writer *resp.Writer, args []resp.RESP)

	// minArgsNum represents the minimal number of arguments that command accepts.
	minArgsNum int

	// persist indicates whether this command needs to be persisted.
	// effective when `appendonly` is true.
	persist bool
}

// cmdTable is the list of all available commands.
var cmdTable = []*Command{
	{"set", setCommand, 2, true},
	{"get", getCommand, 1, false},
	{"del", delCommand, 1, true},
	{"incr", incrCommand, 1, true},
	{"hset", hsetCommand, 3, true},
	{"hget", hgetCommand, 2, false},
	{"hdel", hdelCommand, 2, true},
	{"hgetall", hgetallCommand, 1, false},
	{"rpush", rpushCommand, 2, true},
	{"lpush", lpushCommand, 2, true},
	{"rpop", rpopCommand, 1, true},
	{"lpop", lpopCommand, 1, true},
	{"lrange", lrangeCommand, 3, false},
	{"sadd", saddCommand, 2, true},
	{"srem", sremCommand, 2, true},
	{"spop", spopCommand, 1, true},
	{"smembers", smembersCommand, 1, false},
	{"zadd", zaddCommand, 3, true},
	{"zrem", zremCommand, 2, true},
	{"zrank", zrankCommand, 2, false},
	{"zpopmin", zpopminCommand, 1, true},
	{"zrange", zrangeCommand, 3, false},
	{"eval", evalCommand, 2, true},
	{"ping", pingCommand, 0, false},
	{"flushdb", flushdbCommand, 0, true},
	{"load", loadCommand, 0, false},
	{"save", saveCommand, 0, false},
}

func equalFold(a, b string) bool {
	return len(a) == len(b) && strings.EqualFold(a, b)
}

func lookupCommand(name string) (*Command, error) {
	for _, c := range cmdTable {
		if equalFold(name, c.name) {
			return c, nil
		}
	}
	return nil, fmt.Errorf("%w '%s'", errUnknownCommand, name)
}

func (cmd *Command) process(writer *resp.Writer, args []resp.RESP) {
	if len(args) < cmd.minArgsNum {
		writer.WriteError(errWrongArguments)
		return
	}
	cmd.handler(writer, args)
}

func pingCommand(writer *resp.Writer, _ []resp.RESP) {
	writer.WriteSString("PONG")
}

func setCommand(writer *resp.Writer, args []resp.RESP) {
	key := args[0].ToString()
	value := args[1].Clone()
	extra := args[2:]
	var ttl int64

	for len(extra) > 0 {
		arg := extra[0].ToStringUnsafe()

		// EX
		if equalFold(arg, EX) && len(extra) >= 2 {
			n, err := extra[1].ToDuration()
			if err != nil {
				writer.WriteError(errParseInteger)
				return
			}
			ttl = timer.GetNanoTime() + int64(n*time.Second)
			extra = extra[2:]

			// PX
		} else if equalFold(arg, PX) && len(extra) >= 2 {
			n, err := extra[1].ToDuration()
			if err != nil {
				writer.WriteError(errParseInteger)
				return
			}
			ttl = timer.GetNanoTime() + int64(n*time.Millisecond)
			extra = extra[2:]

			// KEEPTTL
		} else if equalFold(arg, KeepTtl) {
			extra = extra[1:]
			ttl = -1

			// NX
		} else if equalFold(arg, NX) {
			if _, ttl := db.dict.Get(key); ttl != KEY_NOT_EXIST {
				writer.WriteNull()
				return
			}
			extra = extra[1:]

		} else {
			writer.WriteError(errSyntax)
			return
		}
	}

	db.dict.SetWithTTL(key, value, ttl)
	writer.WriteSString("OK")
}

func incrCommand(writer *resp.Writer, args []resp.RESP) {
	key := args[0].ToString()
	object, ttl := db.dict.Get(key)
	if ttl == KEY_NOT_EXIST {
		object = 0
	}
	switch v := object.(type) {
	case int:
		num := v + 1
		writer.WriteInteger(num)
		db.dict.Set(key, num)
	case []byte:
		// conv to integer
		num, err := resp.RESP(v).ToInt()
		if err != nil {
			writer.WriteError(errParseInteger)
			return
		}
		num++
		strconv.AppendInt(v[:0], int64(num), 10)
		writer.WriteInteger(num)
	default:
		writer.WriteError(errWrongType)
	}
}

func getCommand(writer *resp.Writer, args []resp.RESP) {
	key := args[0].ToStringUnsafe()
	object, ttl := db.dict.Get(key)
	if ttl == KEY_NOT_EXIST {
		writer.WriteNull()
		return
	}
	switch v := object.(type) {
	case int:
		writer.WriteBulkString(strconv.Itoa(v))
	case []byte:
		writer.WriteBulk(v)
	default:
		writer.WriteError(errWrongType)
	}
}

func delCommand(writer *resp.Writer, args []resp.RESP) {
	var count int
	for _, arg := range args {
		if db.dict.Delete(arg.ToStringUnsafe()) {
			count++
		}
	}
	writer.WriteInteger(count)
}

func hsetCommand(writer *resp.Writer, args []resp.RESP) {
	key := args[0]
	args = args[1:]
	if len(args)%2 == 1 {
		writer.WriteError(errWrongArguments)
		return
	}
	hmap, err := fetchMap(key, true)
	if err != nil {
		writer.WriteError(err)
		return
	}
	var count int
	for i := 0; i < len(args); i += 2 {
		field := args[i].ToString()
		value := args[i+1].Clone()
		if hmap.Set(field, value) {
			count++
		}
	}
	writer.WriteInteger(count)
}

func hgetCommand(writer *resp.Writer, args []resp.RESP) {
	key := args[0]
	field := args[1].ToStringUnsafe()
	hmap, err := fetchMap(key)
	if err != nil {
		writer.WriteError(errWrongType)
		return
	}
	value, ok := hmap.Get(field)
	if ok {
		writer.WriteBulk(value)
	} else {
		writer.WriteNull()
	}
}

func hdelCommand(writer *resp.Writer, args []resp.RESP) {
	key := args[0]
	fields := args[1:]
	hmap, err := fetchMap(key)
	if err != nil {
		writer.WriteError(err)
		return
	}
	var count int
	for _, field := range fields {
		if hmap.Remove(field.ToStringUnsafe()) {
			count++
		}
	}
	writer.WriteInteger(count)
}

func hgetallCommand(writer *resp.Writer, args []resp.RESP) {
	key := args[0]
	hmap, err := fetchMap(key)
	if err != nil {
		writer.WriteError(err)
		return
	}
	writer.WriteArrayHead(hmap.Len() * 2)
	hmap.Scan(func(key string, value []byte) {
		writer.WriteBulkString(key)
		writer.WriteBulk(value)
	})
}

func lpushCommand(writer *resp.Writer, args []resp.RESP) {
	key := args[0]
	ls, err := fetchList(key, true)
	if err != nil {
		writer.WriteError(err)
		return
	}
	keys := make([]string, 0, len(args)-1)
	for _, arg := range args[1:] {
		keys = append(keys, arg.ToStringUnsafe())
	}
	ls.LPush(keys...)
	writer.WriteInteger(ls.Size())
}

func rpushCommand(writer *resp.Writer, args []resp.RESP) {
	key := args[0]
	ls, err := fetchList(key, true)
	if err != nil {
		writer.WriteError(err)
		return
	}
	keys := make([]string, 0, len(args)-1)
	for _, arg := range args[1:] {
		keys = append(keys, arg.ToStringUnsafe())
	}
	ls.RPush(keys...)
	writer.WriteInteger(ls.Size())
}

func lpopCommand(writer *resp.Writer, args []resp.RESP) {
	key := args[0]
	ls, err := fetchList(key)
	if err != nil {
		writer.WriteError(err)
		return
	}
	val, ok := ls.LPop()
	if ok {
		writer.WriteBulkString(val)
	} else {
		writer.WriteNull()
	}
}

func rpopCommand(writer *resp.Writer, args []resp.RESP) {
	key := args[0]
	ls, err := fetchList(key)
	if err != nil {
		writer.WriteError(err)
		return
	}
	val, ok := ls.RPop()
	if ok {
		writer.WriteBulkString(val)
	} else {
		writer.WriteNull()
	}
}

func lrangeCommand(writer *resp.Writer, args []resp.RESP) {
	key := args[0]
	start, err := args[1].ToInt()
	if err != nil {
		writer.WriteError(err)
		return
	}
	stop, err := args[2].ToInt()
	if err != nil {
		writer.WriteError(err)
		return
	}
	ls, err := fetchList(key)
	if err != nil {
		writer.WriteError(err)
		return
	}

	count := ls.RangeCount(start, stop)
	writer.WriteArrayHead(count)
	ls.Range(start, func(data []byte) (stop bool) {
		if count == 0 {
			return true
		}
		count--
		writer.WriteBulk(data)
		return false
	})
}

func saddCommand(writer *resp.Writer, args []resp.RESP) {
	key := args[0]
	set, err := fetchSet(key, true)
	if err != nil {
		writer.WriteError(err)
		return
	}
	var count int
	for _, arg := range args[1:] {
		if set.Add(arg.ToString()) {
			count++
		}
	}
	writer.WriteInteger(count)
}

func sremCommand(writer *resp.Writer, args []resp.RESP) {
	key := args[0]
	set, err := fetchSet(key)
	if err != nil {
		writer.WriteError(err)
		return
	}
	var count int
	for _, arg := range args[1:] {
		if set.Remove(arg.ToStringUnsafe()) {
			count++
		}
	}
	writer.WriteInteger(count)
}

func smembersCommand(writer *resp.Writer, args []resp.RESP) {
	key := args[0]
	set, err := fetchSet(key)
	if err != nil {
		writer.WriteError(err)
		return
	}
	writer.WriteArrayHead(set.Len())
	set.Scan(func(key string) {
		writer.WriteBulkString(key)
	})
}

func spopCommand(writer *resp.Writer, args []resp.RESP) {
	key := args[0]
	set, err := fetchSet(key)
	if err != nil {
		writer.WriteError(err)
		return
	}
	member, ok := set.Pop()
	if ok {
		writer.WriteBulkString(member)
	} else {
		writer.WriteNull()
	}
}

func zaddCommand(writer *resp.Writer, args []resp.RESP) {
	key := args[0]
	args = args[1:]
	zs, err := fetchZSet(key, true)
	if err != nil {
		writer.WriteError(err)
		return
	}
	var count int
	for i := 0; i < len(args); i += 2 {
		score, err := args[i].ToFloat()
		if err != nil {
			writer.WriteError(err)
			return
		}
		key := args[i+1].ToString()
		if zs.Set(key, score) {
			count++
		}
	}
	writer.WriteInteger(count)
}

func zrankCommand(writer *resp.Writer, args []resp.RESP) {
	key := args[0]
	member := args[1].ToStringUnsafe()
	zs, err := fetchZSet(key)
	if err != nil {
		writer.WriteError(err)
		return
	}
	rank := zs.Rank(member)
	if rank < 0 {
		writer.WriteNull()
	} else {
		writer.WriteInteger(rank)
	}
}

func zremCommand(writer *resp.Writer, args []resp.RESP) {
	key := args[0]
	zs, err := fetchZSet(key)
	if err != nil {
		writer.WriteError(err)
		return
	}
	var count int
	for _, arg := range args[1:] {
		if zs.Remove(arg.ToStringUnsafe()) {
			count++
		}
	}
	writer.WriteInteger(count)
}

func zrangeCommand(writer *resp.Writer, args []resp.RESP) {
	key := args[0]
	start, err := args[1].ToInt()
	if err != nil {
		writer.WriteError(err)
		return
	}
	stop, err := args[2].ToInt()
	if err != nil {
		writer.WriteError(err)
		return
	}
	zs, err := fetchZSet(key)
	if err != nil {
		writer.WriteError(err)
		return
	}

	if stop == -1 {
		stop = zs.Len()
	}
	start = min(start, stop)

	withScores := len(args) == 4 && equalFold(args[3].ToStringUnsafe(), WithScores)
	if withScores {
		writer.WriteArrayHead((stop - start) * 2)
		zs.Scan(func(key string, score float64) {
			if start <= 0 && stop >= 0 {
				writer.WriteBulkString(key)
				writer.WriteFloat(score)
			}
			start--
			stop--
		})

	} else {
		writer.WriteArrayHead(stop - start)
		zs.Scan(func(key string, _ float64) {
			if start <= 0 && stop >= 0 {
				writer.WriteBulkString(key)
			}
			start--
			stop--
		})
	}
}

func zpopminCommand(writer *resp.Writer, args []resp.RESP) {
	key := args[0]
	count := 1
	var err error
	if len(args) > 1 {
		count, err = args[1].ToInt()
		if err != nil {
			writer.WriteError(err)
			return
		}
	}
	zs, err := fetchZSet(key)
	if err != nil {
		writer.WriteError(err)
		return
	}
	size := min(zs.Len(), count)
	writer.WriteArrayHead(size * 2)
	for range size {
		key, score := zs.PopMin()
		writer.WriteBulkString(key)
		writer.WriteFloat(score)
	}
}

func flushdbCommand(writer *resp.Writer, _ []resp.RESP) {
	db.dict = New()
	writer.WriteSString("OK")
}

func loadCommand(writer *resp.Writer, _ []resp.RESP) {
	db.dict = New()
	if err := db.rdb.LoadDB(); err != nil {
		writer.WriteError(err)
		return
	}
	writer.WriteSString("OK")
}

func saveCommand(writer *resp.Writer, _ []resp.RESP) {
	if err := db.rdb.SaveDB(); err != nil {
		writer.WriteError(err)
		return
	}
	writer.WriteSString("OK")
}

func evalCommand(writer *resp.Writer, args []resp.RESP) {
	L := server.lua
	script := args[0].ToString()

	if err := L.DoString(script); err != nil {
		writer.WriteError(err)
		return
	}

	var serialize func(isRoot bool, ret lua.LValue)
	serialize = func(isRoot bool, ret lua.LValue) {
		switch res := ret.(type) {
		case lua.LString:
			writer.WriteBulkString(res.String())

		case lua.LNumber:
			writer.WriteInteger(int(res))

		case *lua.LTable:
			writer.WriteArrayHead(res.Len())
			res.ForEach(func(index, value lua.LValue) {
				serialize(false, value)
			})

		default:
			writer.WriteNull()
		}

		if isRoot && ret.Type() != lua.LTNil {
			L.Pop(1)
		}
	}
	serialize(true, L.Get(-1))
}

func fetchMap(key []byte, setnx ...bool) (Map, error) {
	return fetch(key, func() Map { return hash.NewZipMap() }, setnx...)
}

func fetchList(key []byte, setnx ...bool) (List, error) {
	return fetch(key, func() List { return list.New() }, setnx...)
}

func fetchSet(key []byte, setnx ...bool) (Set, error) {
	return fetch(key, func() Set { return hash.NewZipSet() }, setnx...)
}

func fetchZSet(key []byte, setnx ...bool) (ZSet, error) {
	return fetch(key, func() ZSet { return zset.NewZipZSet() }, setnx...)
}

func fetch[T any](key []byte, new func() T, setnx ...bool) (T, error) {
	object, ttl := db.dict.Get(b2s(key))
	if ttl != KEY_NOT_EXIST {
		v, ok := object.(T)
		if !ok {
			return v, errWrongType
		}

		// conversion zipped structure
		if len(setnx) > 0 && setnx[0] {
			switch data := object.(type) {
			case *hash.ZipMap:
				if data.Len() >= 256 {
					db.dict.Set(string(key), data.ToMap())
				}
			case *hash.ZipSet:
				if data.Len() >= 512 {
					db.dict.Set(string(key), data.ToSet())
				}
			case *zset.ZipZSet:
				if data.Len() >= 256 {
					db.dict.Set(string(key), data.ToZSet())
				}
			}
		}
		return v, nil
	}

	v := new()
	if len(setnx) > 0 && setnx[0] {
		db.dict.Set(string(key), v)
	}

	return v, nil
}

func b2s(b []byte) string { return *(*string)(unsafe.Pointer(&b)) }

func getObjectType(object any) ObjectType {
	switch object.(type) {
	case []byte:
		return TypeString
	case int:
		return TypeInteger
	case *hash.Map:
		return TypeMap
	case *hash.ZipMap:
		return TypeZipMap
	case *hash.Set:
		return TypeSet
	case *hash.ZipSet:
		return TypeZipSet
	case *list.QuickList:
		return TypeList
	case *zset.ZSet:
		return TypeZSet
	case *zset.ZipZSet:
		return TypeZipZSet
	default:
		panic("unknown type")
	}
	return TypeUnknown
}
