package main

import (
	"bytes"
	"fmt"
	"github.com/tidwall/redcon"
	"github.com/xgzlucario/rotom/internal/resp"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/xgzlucario/rotom/internal/hash"
	"github.com/xgzlucario/rotom/internal/list"
	"github.com/xgzlucario/rotom/internal/zset"
)

const (
	KeepTtl    = "KEEPTTL"
	Count      = "COUNT"
	Match      = "MATCH"
	NX         = "NX"
	EX         = "EX"
	PX         = "PX"
	WithScores = "WITHSCORES"
)

type Command struct {
	// name is lowercase letters command name.
	name string

	// handler is this command real database handler function.
	handler func(writer *resp.Writer, args []redcon.RESP)

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
	{"scan", scanCommand, 1, false},
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
	{"ping", pingCommand, 0, false},
	{"hello", helloCommand, 0, false},
	{"flushdb", flushdbCommand, 0, true},
	//{"load", loadCommand, 0, false},
	//{"save", saveCommand, 0, false},
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

func (cmd *Command) process(writer *resp.Writer, args []redcon.RESP) {
	if len(args) < cmd.minArgsNum {
		writer.WriteError(errWrongArguments.Error())
		return
	}
	cmd.handler(writer, args)
}

func pingCommand(writer *resp.Writer, _ []redcon.RESP) {
	writer.WriteString("PONG")
}

func setCommand(writer *resp.Writer, args []redcon.RESP) {
	key := b2s(args[0].Bytes())
	extra := args[2:]
	var ttl int64

	_, ttl = db.dict.Get(key)
	if ttl == KeyNotExist {
		key = args[0].String() // copy
	}

	for len(extra) > 0 {
		arg := b2s(extra[0].Bytes())
		// EX
		if equalFold(arg, EX) && len(extra) >= 2 {
			n := extra[1].Int()
			ttl = time.Now().Add(time.Duration(n) * time.Second).UnixNano()
			extra = extra[2:]
			// PX
		} else if equalFold(arg, PX) && len(extra) >= 2 {
			n := extra[1].Int()
			ttl = time.Now().Add(time.Duration(n) * time.Millisecond).UnixNano()
			extra = extra[2:]
			// KEEPTTL
		} else if equalFold(arg, KeepTtl) {
			extra = extra[1:]
			ttl = -1
			// NX
		} else if equalFold(arg, NX) {
			if _, ttl := db.dict.Get(key); ttl != KeyNotExist {
				writer.WriteNull()
				return
			}
			extra = extra[1:]
		} else {
			writer.WriteError(errSyntax.Error())
			return
		}
	}

	value := bytes.Clone(args[1].Bytes())
	db.dict.SetWithTTL(key, value, ttl)
	writer.WriteString("OK")
}

func incrCommand(writer *resp.Writer, args []redcon.RESP) {
	key := args[0].String()
	object, ttl := db.dict.Get(key)
	if ttl == KeyNotExist {
		object = 0
	}
	switch v := object.(type) {
	case int:
		num := v + 1
		writer.WriteInt(num)
		db.dict.Set(key, num)
	case []byte:
		// conv to integer
		num, err := strconv.Atoi(b2s(v))
		if err != nil {
			writer.WriteError(errParseInteger.Error())
			return
		}
		num++
		strconv.AppendInt(v[:0], int64(num), 10)
		writer.WriteInt(num)
	default:
		writer.WriteError(errWrongType.Error())
	}
}

func getCommand(writer *resp.Writer, args []redcon.RESP) {
	key := b2s(args[0].Bytes())
	object, ttl := db.dict.Get(key)
	if ttl == KeyNotExist {
		writer.WriteNull()
		return
	}
	switch v := object.(type) {
	case int:
		writer.WriteBulkString(strconv.Itoa(v))
	case []byte:
		writer.WriteBulk(v)
	default:
		writer.WriteError(errWrongType.Error())
	}
}

func delCommand(writer *resp.Writer, args []redcon.RESP) {
	var count int
	for _, arg := range args {
		if db.dict.Delete(b2s(arg.Bytes())) {
			count++
		}
	}
	writer.WriteInt(count)
}

func scanCommand(writer *resp.Writer, args []redcon.RESP) {
	cursor := int(args[0].Int())
	count := 10
	extra := args[1:]

	for len(extra) > 0 {
		arg := b2s(extra[0].Bytes())
		// COUNT
		if equalFold(arg, Count) && len(extra) >= 2 {
			count = int(extra[1].Int())
			extra = extra[2:]
		} else {
			writer.WriteError(errSyntax.Error())
			return
		}
	}

	keys := make([]string, 0, count)
	db.dict.data.All(func(key string, _ any) bool {
		keys = append(keys, key)
		return count > len(keys)
	})

	if len(keys) == db.dict.data.Len() {
		cursor = 0
	} else {
		cursor = count
	}
	writer.WriteArray(2)
	writer.WriteInt(cursor)
	writer.WriteAny(keys)
}

func hsetCommand(writer *resp.Writer, args []redcon.RESP) {
	key := args[0].Bytes()
	args = args[1:]
	if len(args)%2 == 1 {
		writer.WriteError(errWrongArguments.Error())
		return
	}
	hmap, err := fetchMap(key, true)
	if err != nil {
		writer.WriteError(err.Error())
		return
	}
	var count int
	for i := 0; i < len(args); i += 2 {
		field := args[i].String()
		value := args[i+1].Bytes() // no need to clone
		if hmap.Set(field, value) {
			count++
		}
	}
	writer.WriteInt(count)
}

func hgetCommand(writer *resp.Writer, args []redcon.RESP) {
	key := args[0].Bytes()
	field := b2s(args[1].Bytes())
	hmap, err := fetchMap(key)
	if err != nil {
		writer.WriteError(errWrongType.Error())
		return
	}
	value, ok := hmap.Get(field)
	if ok {
		writer.WriteBulk(value)
	} else {
		writer.WriteNull()
	}
}

func hdelCommand(writer *resp.Writer, args []redcon.RESP) {
	key := args[0].Bytes()
	fields := args[1:]
	hmap, err := fetchMap(key)
	if err != nil {
		writer.WriteError(err.Error())
		return
	}
	var count int
	for _, field := range fields {
		if hmap.Remove(b2s(field.Bytes())) {
			count++
		}
	}
	writer.WriteInt(count)
}

func hgetallCommand(writer *resp.Writer, args []redcon.RESP) {
	key := args[0].Bytes()
	hmap, err := fetchMap(key)
	if err != nil {
		writer.WriteError(err.Error())
		return
	}
	writer.WriteArray(hmap.Len() * 2)
	hmap.Scan(func(key string, value []byte) {
		writer.WriteBulkString(key)
		writer.WriteBulk(value)
	})
}

func lpushCommand(writer *resp.Writer, args []redcon.RESP) {
	key := args[0].Bytes()
	ls, err := fetchList(key, true)
	if err != nil {
		writer.WriteError(err.Error())
		return
	}
	keys := make([]string, 0, len(args)-1)
	for _, arg := range args[1:] {
		keys = append(keys, b2s(arg.Bytes()))
	}
	ls.LPush(keys...)
	writer.WriteInt(ls.Len())
}

func rpushCommand(writer *resp.Writer, args []redcon.RESP) {
	key := args[0].Bytes()
	ls, err := fetchList(key, true)
	if err != nil {
		writer.WriteError(err.Error())
		return
	}
	keys := make([]string, 0, len(args)-1)
	for _, arg := range args[1:] {
		keys = append(keys, b2s(arg.Bytes()))
	}
	ls.RPush(keys...)
	writer.WriteInt(ls.Len())
}

func lpopCommand(writer *resp.Writer, args []redcon.RESP) {
	key := args[0].Bytes()
	ls, err := fetchList(key)
	if err != nil {
		writer.WriteError(err.Error())
		return
	}
	val, ok := ls.LPop()
	if ok {
		writer.WriteBulkString(val)
	} else {
		writer.WriteNull()
	}
}

func rpopCommand(writer *resp.Writer, args []redcon.RESP) {
	key := args[0].Bytes()
	ls, err := fetchList(key)
	if err != nil {
		writer.WriteError(err.Error())
		return
	}
	val, ok := ls.RPop()
	if ok {
		writer.WriteBulkString(val)
	} else {
		writer.WriteNull()
	}
}

func lrangeCommand(writer *resp.Writer, args []redcon.RESP) {
	key := args[0].Bytes()
	start := int(args[1].Int())
	stop := int(args[2].Int())

	ls, err := fetchList(key)
	if err != nil {
		writer.WriteError(err.Error())
		return
	}

	count := ls.RangeCount(start, stop)
	writer.WriteArray(count)
	ls.Range(start, func(data []byte) (stop bool) {
		if count == 0 {
			return true
		}
		count--
		writer.WriteBulk(data)
		return false
	})
}

func saddCommand(writer *resp.Writer, args []redcon.RESP) {
	key := args[0].Bytes()
	set, err := fetchSet(key, true)
	if err != nil {
		writer.WriteError(err.Error())
		return
	}
	var count int
	for _, arg := range args[1:] {
		if set.Add(arg.String()) {
			count++
		}
	}
	writer.WriteInt(count)
}

func sremCommand(writer *resp.Writer, args []redcon.RESP) {
	key := args[0].Bytes()
	set, err := fetchSet(key)
	if err != nil {
		writer.WriteError(err.Error())
		return
	}
	var count int
	for _, arg := range args[1:] {
		if set.Remove(b2s(arg.Bytes())) {
			count++
		}
	}
	writer.WriteInt(count)
}

func smembersCommand(writer *resp.Writer, args []redcon.RESP) {
	key := args[0].Bytes()
	set, err := fetchSet(key)
	if err != nil {
		writer.WriteError(err.Error())
		return
	}
	writer.WriteArray(set.Len())
	set.Scan(func(key string) {
		writer.WriteBulkString(key)
	})
}

func spopCommand(writer *resp.Writer, args []redcon.RESP) {
	key := args[0].Bytes()
	set, err := fetchSet(key)
	if err != nil {
		writer.WriteError(err.Error())
		return
	}
	member, ok := set.Pop()
	if ok {
		writer.WriteBulkString(member)
	} else {
		writer.WriteNull()
	}
}

func zaddCommand(writer *resp.Writer, args []redcon.RESP) {
	key := args[0].Bytes()
	args = args[1:]
	zs, err := fetchZSet(key, true)
	if err != nil {
		writer.WriteError(err.Error())
		return
	}
	var count int
	for i := 0; i < len(args); i += 2 {
		score := args[i].Float()
		key := args[i+1].String()
		if zs.Set(key, score) {
			count++
		}
	}
	writer.WriteInt(count)
}

func zrankCommand(writer *resp.Writer, args []redcon.RESP) {
	key := args[0].Bytes()
	member := b2s(args[1].Bytes())
	zs, err := fetchZSet(key)
	if err != nil {
		writer.WriteError(err.Error())
		return
	}
	rank := zs.Rank(member)
	if rank < 0 {
		writer.WriteNull()
	} else {
		writer.WriteInt(rank)
	}
}

func zremCommand(writer *resp.Writer, args []redcon.RESP) {
	key := args[0].Bytes()
	zs, err := fetchZSet(key)
	if err != nil {
		writer.WriteError(err.Error())
		return
	}
	var count int
	for _, arg := range args[1:] {
		if zs.Remove(b2s(arg.Bytes())) {
			count++
		}
	}
	writer.WriteInt(count)
}

func zrangeCommand(writer *resp.Writer, args []redcon.RESP) {
	key := args[0].Bytes()
	start := int(args[1].Int())
	stop := int(args[2].Int())

	zs, err := fetchZSet(key)
	if err != nil {
		writer.WriteError(err.Error())
		return
	}
	if stop == -1 {
		stop = zs.Len()
	}
	start = min(start, stop)

	withScores := len(args) == 4 && equalFold(b2s(args[3].Bytes()), WithScores)
	if withScores {
		writer.WriteArray((stop - start) * 2)
	} else {
		writer.WriteArray(stop - start)
	}
	zs.Scan(func(key string, score float64) {
		if start <= 0 && stop >= 0 {
			writer.WriteBulkString(key)
			if withScores {
				writer.WriteAny(score)
			}
		}
		start--
		stop--
	})
}

func zpopminCommand(writer *resp.Writer, args []redcon.RESP) {
	key := args[0].Bytes()
	count := 1
	if len(args) > 1 {
		count = int(args[1].Int())
	}
	zs, err := fetchZSet(key)
	if err != nil {
		writer.WriteError(err.Error())
		return
	}
	n := min(zs.Len(), count)
	writer.WriteArray(n * 2)
	for range n {
		kstr, score := zs.PopMin()
		writer.WriteBulkString(kstr)
		writer.WriteAny(score)
	}
}

func flushdbCommand(writer *resp.Writer, _ []redcon.RESP) {
	db.dict = New()
	writer.WriteString("OK")
}

func helloCommand(writer *resp.Writer, _ []redcon.RESP) {
	writer.WriteAny(map[string]any{
		"server":  "rotom",
		"version": "1.0.0",
		"proto":   2,
		"mode":    "standalone",
		"role":    "master",
	})
}

//func loadCommand(writer *resp.Writer, _ []redcon.RESP) {
//	db.dict = New()
//	if err := db.rdb.LoadDB(); err != nil {
//		writer.WriteError(err.Error())
//		return
//	}
//	writer.WriteString("OK")
//}
//
//func saveCommand(writer *resp.Writer, _ []redcon.RESP) {
//	if err := db.rdb.SaveDB(); err != nil {
//		writer.WriteError(err.Error())
//		return
//	}
//	writer.WriteString("OK")
//}

func fetchMap(key []byte, setnx ...bool) (Map, error) {
	return fetch(key, func() Map { return hash.New() }, setnx...)
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
	if ttl != KeyNotExist {
		v, ok := object.(T)
		if !ok {
			return v, errWrongType
		}

		// conversion zipped structure
		if len(setnx) > 0 && setnx[0] {
			switch data := object.(type) {
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
	case *hash.ZipMap:
		return TypeMap
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
