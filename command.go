package main

import (
	"fmt"
	"github.com/bytedance/sonic"
	"os"
	"strconv"
	"strings"
	"time"

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
	handler func(writer *RESPWriter, args []RESP)

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
	{"zadd", zaddCommand, 3, true},
	{"zrem", zremCommand, 2, true},
	{"zrank", zrankCommand, 2, false},
	{"zpopmin", zpopminCommand, 1, true},
	{"zrange", zrangeCommand, 3, false},
	{"eval", evalCommand, 2, true},
	{"ping", pingCommand, 0, false},
	{"flushdb", flushdbCommand, 0, true},
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

func (cmd *Command) process(writer *RESPWriter, args []RESP) {
	if len(args) < cmd.minArgsNum {
		writer.WriteError(errWrongArguments)
		return
	}
	cmd.handler(writer, args)
}

func pingCommand(writer *RESPWriter, _ []RESP) {
	writer.WriteString("PONG")
}

func setCommand(writer *RESPWriter, args []RESP) {
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
			ttl = time.Now().Add(n * time.Second).UnixNano()
			extra = extra[2:]

			// PX
		} else if equalFold(arg, PX) && len(extra) >= 2 {
			n, err := extra[1].ToDuration()
			if err != nil {
				writer.WriteError(errParseInteger)
				return
			}
			ttl = time.Now().Add(n * time.Millisecond).UnixNano()
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
	writer.WriteString("OK")
}

func incrCommand(writer *RESPWriter, args []RESP) {
	key := args[0].ToStringUnsafe()

	object, ttl := db.dict.Get(key)
	if ttl == KEY_NOT_EXIST {
		db.dict.Set(strings.Clone(key), 1)
		writer.WriteInteger(1)
		return
	}

	switch v := object.(type) {
	case int:
		num := v + 1
		writer.WriteInteger(num)
		db.dict.Set(strings.Clone(key), num)

	case []byte:
		// conv to integer
		num, err := RESP(v).ToInt()
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

func getCommand(writer *RESPWriter, args []RESP) {
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

func delCommand(writer *RESPWriter, args []RESP) {
	var count int
	for _, arg := range args {
		if db.dict.Delete(arg.ToStringUnsafe()) {
			count++
		}
	}
	writer.WriteInteger(count)
}

func hsetCommand(writer *RESPWriter, args []RESP) {
	hash := args[0]
	args = args[1:]

	if len(args)%2 == 1 {
		writer.WriteError(errWrongArguments)
		return
	}

	hmap, err := fetchMap(hash, true)
	if err != nil {
		writer.WriteError(err)
		return
	}

	var count int
	for i := 0; i < len(args); i += 2 {
		key := args[i].ToString()
		value := args[i+1].Clone()
		if hmap.Set(key, value) {
			count++
		}
	}
	writer.WriteInteger(count)
}

func hgetCommand(writer *RESPWriter, args []RESP) {
	hash := args[0]
	key := args[1].ToStringUnsafe()
	hmap, err := fetchMap(hash)
	if err != nil {
		writer.WriteError(errWrongType)
		return
	}
	value, ok := hmap.Get(key)
	if ok {
		writer.WriteBulk(value)
	} else {
		writer.WriteNull()
	}
}

func hdelCommand(writer *RESPWriter, args []RESP) {
	hash := args[0]
	keys := args[1:]
	hmap, err := fetchMap(hash)
	if err != nil {
		writer.WriteError(err)
		return
	}
	var count int
	for _, v := range keys {
		if hmap.Remove(v.ToStringUnsafe()) {
			count++
		}
	}
	writer.WriteInteger(count)
}

func hgetallCommand(writer *RESPWriter, args []RESP) {
	hash := args[0]
	hmap, err := fetchMap(hash)
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

func lpushCommand(writer *RESPWriter, args []RESP) {
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

func rpushCommand(writer *RESPWriter, args []RESP) {
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

func lpopCommand(writer *RESPWriter, args []RESP) {
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

func rpopCommand(writer *RESPWriter, args []RESP) {
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

func lrangeCommand(writer *RESPWriter, args []RESP) {
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
	writer.WriteArrayHead(ls.RangeCount(start, stop))
	ls.Range(start, stop, func(data []byte) {
		writer.WriteBulk(data)
	})
}

func saddCommand(writer *RESPWriter, args []RESP) {
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

func sremCommand(writer *RESPWriter, args []RESP) {
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

func spopCommand(writer *RESPWriter, args []RESP) {
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

func zaddCommand(writer *RESPWriter, args []RESP) {
	key := args[0]
	args = args[1:]

	zset, err := fetchZSet(key, true)
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
		if zset.Set(key, score) {
			count++
		}
	}
	writer.WriteInteger(count)
}

func zrankCommand(writer *RESPWriter, args []RESP) {
	key := args[0]
	member := args[1].ToStringUnsafe()

	zset, err := fetchZSet(key)
	if err != nil {
		writer.WriteError(err)
		return
	}

	rank, _ := zset.Rank(member)
	if rank < 0 {
		writer.WriteNull()
	} else {
		writer.WriteInteger(rank)
	}
}

func zremCommand(writer *RESPWriter, args []RESP) {
	key := args[0]
	zset, err := fetchZSet(key)
	if err != nil {
		writer.WriteError(err)
		return
	}
	var count int
	for _, arg := range args[1:] {
		if zset.Remove(arg.ToStringUnsafe()) {
			count++
		}
	}
	writer.WriteInteger(count)
}

func zrangeCommand(writer *RESPWriter, args []RESP) {
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
	zset, err := fetchZSet(key)
	if err != nil {
		writer.WriteError(err)
		return
	}

	if stop == -1 {
		stop = zset.Len()
	}
	start = min(start, stop)

	withScores := len(args) == 4 && equalFold(args[3].ToStringUnsafe(), WithScores)
	if withScores {
		writer.WriteArrayHead((stop - start) * 2)
		zset.Range(start, stop, func(key string, score float64) {
			writer.WriteBulkString(key)
			writer.WriteFloat(score)
		})

	} else {
		writer.WriteArrayHead(stop - start)
		zset.Range(start, stop, func(key string, _ float64) {
			writer.WriteBulkString(key)
		})
	}
}

func zpopminCommand(writer *RESPWriter, args []RESP) {
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

func flushdbCommand(writer *RESPWriter, _ []RESP) {
	db.dict = New()
	writer.WriteString("OK")
}

type KVEntry struct {
	Type ObjectType `json:"o"`
	Key  string     `json:"k"`
	Ttl  int64      `json:"t,omitempty"`
	Data any        `json:"v"`
}

func saveCommand(writer *RESPWriter, _ []RESP) {
	dbWriter := NewWriter(1024)
	fs, err := os.Create(server.config.SaveFileName)
	if err != nil {
		writer.WriteError(err)
		return
	}

	batchSize := 100
	dbWriter.WriteArrayHead(len(db.dict.data)/batchSize + 1)

	var entries []KVEntry
	for k, v := range db.dict.data {
		entries = append(entries, KVEntry{
			Type: getObjectType(v),
			Key:  k,
			Ttl:  db.dict.expire[k],
			Data: v,
		})
		if len(entries) == batchSize {
			bytes, _ := sonic.Marshal(entries)
			dbWriter.WriteBulk(bytes)
			entries = entries[:0]
		}
	}
	bytes, _ := sonic.Marshal(entries)
	dbWriter.WriteBulk(bytes)

	_, err = dbWriter.FlushTo(fs)
	if err != nil {
		writer.WriteError(err)
		return
	}
	_ = fs.Close()
	writer.WriteBulkString("OK")
}

func evalCommand(writer *RESPWriter, args []RESP) {
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
			writer.WriteInteger(int(res)) // convert to integer

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
	return fetch(key, func() ZSet { return zset.NewZSet() }, setnx...)
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
				if data.Len() < 256 {
					break
				}
				db.dict.Set(string(key), data.ToMap())

			case *hash.ZipSet:
				if data.Len() < 512 {
					break
				}
				db.dict.Set(string(key), data.ToSet())
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

func getObjectType(object any) ObjectType {
	switch object.(type) {
	case string, []byte:
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
	}
	return TypeUnknown
}
