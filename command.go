package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/xgzlucario/rotom/internal/dict"
	"github.com/xgzlucario/rotom/internal/hash"
	"github.com/xgzlucario/rotom/internal/list"
	"github.com/xgzlucario/rotom/internal/zset"
)

var (
	WITH_SCORES = "WITHSCORES"
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
var cmdTable []*Command = []*Command{
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
	{"ping", pingCommand, 0, false},
	{"flushdb", flushdbCommand, 0, true},
	// TODO
	{"mset", todoCommand, 0, false},
	{"xadd", todoCommand, 0, false},
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

func (cmd *Command) processCommand(writer *RESPWriter, args []RESP) {
	if len(args) < cmd.minArgsNum {
		writer.WriteError(errInvalidArguments)
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
	db.dict.Set(key, value)
	writer.WriteString("OK")
}

func incrCommand(writer *RESPWriter, args []RESP) {
	key := args[0].ToStringUnsafe()

	object, ttl := db.dict.Get(key)
	if ttl == dict.KEY_NOT_EXIST {
		db.dict.Set(strings.Clone(key), 1)
		writer.WriteInteger(1)
		return
	}

	switch object.Type() {
	case dict.TypeInteger:
		num := object.Data().(int) + 1
		object.SetData(num)
		writer.WriteInteger(num)

	case dict.TypeString:
		// conv to integer
		bytes := object.Data().([]byte)
		num, err := RESP(bytes).ToInt()
		if err != nil {
			writer.WriteError(errParseInteger)
			return
		}
		num++
		bytes = bytes[:0]
		bytes = strconv.AppendInt(bytes, int64(num), 10)
		object.SetData(bytes)
		writer.WriteInteger(num)

	default:
		writer.WriteError(errWrongType)
	}
}

func getCommand(writer *RESPWriter, args []RESP) {
	key := args[0].ToStringUnsafe()

	object, ttl := db.dict.Get(key)
	if ttl == dict.KEY_NOT_EXIST {
		writer.WriteNull()
		return
	}

	switch object.Type() {
	case dict.TypeInteger:
		num := object.Data().(int)
		writer.WriteBulkString(strconv.Itoa(num))

	case dict.TypeString:
		bytes := object.Data().([]byte)
		writer.WriteBulk(bytes)

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
	hash := args[0].ToStringUnsafe()
	args = args[1:]

	if len(args)%2 == 1 {
		writer.WriteError(errInvalidArguments)
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
	hash := args[0].ToStringUnsafe()
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
	hash := args[0].ToStringUnsafe()
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
	hash := args[0].ToStringUnsafe()
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
	key := args[0].ToStringUnsafe()
	ls, err := fetchList(key, true)
	if err != nil {
		writer.WriteError(err)
		return
	}
	for _, arg := range args[1:] {
		ls.LPush(arg.ToStringUnsafe())
	}
	writer.WriteInteger(ls.Size())
}

func rpushCommand(writer *RESPWriter, args []RESP) {
	key := args[0].ToStringUnsafe()
	ls, err := fetchList(key, true)
	if err != nil {
		writer.WriteError(err)
		return
	}
	for _, arg := range args[1:] {
		ls.RPush(arg.ToStringUnsafe())
	}
	writer.WriteInteger(ls.Size())
}

func lpopCommand(writer *RESPWriter, args []RESP) {
	key := args[0].ToStringUnsafe()
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
	key := args[0].ToStringUnsafe()
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
	key := args[0].ToStringUnsafe()
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

	if stop == -1 {
		stop = ls.Size()
	}
	start = min(start, stop)

	writer.WriteArrayHead(stop - start)
	ls.Range(start, stop, func(data []byte) {
		writer.WriteBulk(data)
	})
}

func saddCommand(writer *RESPWriter, args []RESP) {
	key := args[0].ToStringUnsafe()
	args = args[1:]

	set, err := fetchSet(key, true)
	if err != nil {
		writer.WriteError(err)
		return
	}

	var count int
	for i := 0; i < len(args); i++ {
		if set.Add(args[i].ToString()) {
			count++
		}
	}
	writer.WriteInteger(count)
}

func sremCommand(writer *RESPWriter, args []RESP) {
	key := args[0].ToStringUnsafe()
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
	key := args[0].ToStringUnsafe()
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
	key := args[0].ToStringUnsafe()
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
	key := args[0].ToStringUnsafe()
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
	key := args[0].ToStringUnsafe()
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
	key := args[0].ToStringUnsafe()
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

	withScores := len(args) == 4 && equalFold(args[3].ToStringUnsafe(), WITH_SCORES)
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
	key := args[0].ToStringUnsafe()
	count := 1
	var err error
	if len(args) > 1 {
		count, err = args[1].ToInt()
		if err != nil {
			writer.WriteError(err)
			return
		}
	}

	zset, err := fetchZSet(key)
	if err != nil {
		writer.WriteError(err)
		return
	}

	size := min(zset.Len(), count)
	writer.WriteArrayHead(size * 2)
	for range size {
		key, score := zset.PopMin()
		writer.WriteBulkString(key)
		writer.WriteFloat(score)
	}
}

func flushdbCommand(writer *RESPWriter, _ []RESP) {
	db.dict = dict.New()
	writer.WriteString("OK")
}

func todoCommand(writer *RESPWriter, _ []RESP) {
	writer.WriteString("OK")
}

func fetchMap(key string, setnx ...bool) (Map, error) {
	return fetch(key, func() Map { return hash.NewZipMap() }, setnx...)
}

func fetchList(key string, setnx ...bool) (List, error) {
	return fetch(key, func() List { return list.New() }, setnx...)
}

func fetchSet(key string, setnx ...bool) (Set, error) {
	return fetch(key, func() Set { return hash.NewZipSet() }, setnx...)
}

func fetchZSet(key string, setnx ...bool) (ZSet, error) {
	return fetch(key, func() ZSet { return zset.NewZSet() }, setnx...)
}

func fetch[T any](key string, new func() T, setnx ...bool) (T, error) {
	object, ttl := db.dict.Get(key)

	if ttl != dict.KEY_NOT_EXIST {
		v, ok := object.Data().(T)
		if !ok {
			return v, errWrongType
		}

		// conversion zipped structure
		if len(setnx) > 0 && setnx[0] {
			switch object.Type() {
			case dict.TypeZipMap:
				zm := object.Data().(*hash.ZipMap)
				if zm.Len() < 256 {
					break
				}
				object.SetData(zm.ToMap())

			case dict.TypeZipSet:
				zm := object.Data().(*hash.ZipSet)
				if zm.Len() < 512 {
					break
				}
				object.SetData(zm.ToSet())
			}
		}

		return v, nil
	}

	v := new()
	if len(setnx) > 0 && setnx[0] {
		// make sure `key` is copy
		db.dict.Set(strings.Clone(key), v)
	}

	return v, nil
}
