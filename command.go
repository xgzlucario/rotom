package main

import (
	"strconv"

	"github.com/xgzlucario/rotom/internal/hash"
	"github.com/xgzlucario/rotom/internal/list"
	"github.com/xgzlucario/rotom/internal/zset"
)

type Command struct {
	// name is lowercase letters command name.
	name string

	// handler is this command real database handler function.
	handler func(writer *RESPWriter, args []RESP)

	// arity represents the minimal number of arguments that command accepts.
	arity int

	// persist indicates whether this command needs to be persisted.
	// effective when `appendonly` is true.
	persist bool
}

// cmdTable is the list of all available commands.
var cmdTable []*Command = []*Command{
	{"set", setCommand, 2, true},
	{"mset", msetCommand, 2, true},
	{"get", getCommand, 1, false},
	{"incr", incrCommand, 1, true},
	{"hset", hsetCommand, 3, true},
	{"hget", hgetCommand, 2, false},
	{"hdel", hdelCommand, 2, true},
	{"rpush", rpushCommand, 2, true},
	{"lpush", lpushCommand, 2, true},
	{"rpop", rpopCommand, 1, true},
	{"lpop", lpopCommand, 1, true},
	{"sadd", saddCommand, 2, true},
	{"srem", sremCommand, 2, true},
	{"spop", spopCommand, 1, true},
	{"zadd", zaddCommand, 3, true},
	{"ping", pingCommand, 0, false},
	{"hgetall", hgetallCommand, 1, false},
	{"lrange", lrangeCommand, 3, false},
	{"zpopmin", todoCommand, 0, false},
	{"xadd", todoCommand, 0, false},
}

func lookupCommand(command string) *Command {
	for _, c := range cmdTable {
		if equalCommand(command, c.name) {
			return c
		}
	}
	return nil
}

func equalCommand(str, lowerText string) bool {
	if len(str) != len(lowerText) {
		return false
	}
	const s = 'a' - 'A'
	for i, lt := range lowerText {
		delta := lt - rune(str[i])
		if delta != 0 && delta != s {
			return false
		}
	}
	return true
}

func (cmd *Command) processCommand(writer *RESPWriter, args []RESP) {
	if len(args) < cmd.arity {
		writer.WriteError(ErrWrongNumberArgs(cmd.name))
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

func msetCommand(writer *RESPWriter, args []RESP) {
	// check arguments number
	if len(args)%2 == 1 {
		writer.WriteError(ErrWrongNumberArgs("mset"))
		return
	}
	for i := 0; i < len(args); i += 2 {
		key := args[i].ToString()
		value := args[i+1].Clone()
		db.dict.Set(key, value)
	}
	writer.WriteString("OK")
}

func incrCommand(writer *RESPWriter, args []RESP) {
	key := args[0].ToString()

	val, ok := db.dict.Get(key)
	if !ok {
		db.dict.Set(key, []byte("1"))
		writer.WriteInteger(1)
		return
	}

	valBytes, ok := val.([]byte)
	if !ok {
		writer.WriteError(ErrWrongType)
		return
	}

	num, err := RESP(valBytes).ToInt()
	if err != nil {
		writer.WriteError(ErrParseInteger)
		return
	}
	num++

	db.dict.Set(key, []byte(strconv.Itoa(num)))
	writer.WriteInteger(num)
}

func getCommand(writer *RESPWriter, args []RESP) {
	key := args[0].ToStringUnsafe()

	val, ok := db.dict.Get(key)
	if !ok {
		writer.WriteNull()
		return
	}

	valBytes, ok := val.([]byte)
	if ok {
		writer.WriteBulk(valBytes)
	} else {
		writer.WriteError(ErrWrongType)
	}
}

func hsetCommand(writer *RESPWriter, args []RESP) {
	hash := args[0].ToString()
	args = args[1:]

	// check arguments number
	if len(args)%2 == 1 {
		writer.WriteError(ErrWrongNumberArgs("hset"))
		return
	}

	hmap, err := fetchMap(hash, true)
	if err != nil {
		writer.WriteError(err)
		return
	}

	var newFields int
	for i := 0; i < len(args); i += 2 {
		key := args[i].ToString()
		value := args[i+1].Clone()
		if hmap.Set(key, value) {
			newFields++
		}
	}
	writer.WriteInteger(newFields)
}

func hgetCommand(writer *RESPWriter, args []RESP) {
	hash := args[0].ToStringUnsafe()
	key := args[1].ToStringUnsafe()

	hmap, err := fetchMap(hash)
	if err != nil {
		writer.WriteError(ErrWrongType)
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
	var success int
	for _, v := range keys {
		if hmap.Remove(v.ToString()) {
			success++
		}
	}
	writer.WriteInteger(success)
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
	key := args[0].ToString()

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
	key := args[0].ToString()

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
	end, err := args[2].ToInt()
	if err != nil {
		writer.WriteError(err)
		return
	}

	ls, err := fetchList(key)
	if err != nil {
		writer.WriteError(err)
		return
	}

	// calculate list size
	size := end - start
	if end == -1 {
		size = ls.Size()
	}
	if size < 0 {
		size = 0
	}

	writer.WriteArrayHead(size)
	ls.Range(start, end, func(data []byte) {
		writer.WriteBulk(data)
	})
}

func saddCommand(writer *RESPWriter, args []RESP) {
	key := args[0].ToString()
	args = args[1:]

	set, err := fetchSet(key, true)
	if err != nil {
		writer.WriteError(err)
		return
	}

	var newItems int
	for i := 0; i < len(args); i++ {
		if set.Add(args[i].ToString()) {
			newItems++
		}
	}
	writer.WriteInteger(newItems)
}

func sremCommand(writer *RESPWriter, args []RESP) {
	key := args[0].ToString()

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
	key := args[0].ToString()

	set, err := fetchSet(key)
	if err != nil {
		writer.WriteError(err)
		return
	}

	item, ok := set.Pop()
	if ok {
		writer.WriteBulkString(item)
	} else {
		writer.WriteNull()
	}
}

func zaddCommand(writer *RESPWriter, args []RESP) {
	key := args[0].ToString()
	args = args[1:]

	zset, err := fetchZSet(key, true)
	if err != nil {
		writer.WriteError(err)
		return
	}

	var newFields int
	for i := 0; i < len(args); i += 2 {
		score, err := args[i].ToInt()
		if err != nil {
			writer.WriteError(err)
			return
		}

		key := args[i+1].ToString()
		if zset.Set(key, int64(score)) {
			newFields++
		}
	}
	writer.WriteInteger(newFields)
}

// TODO
func todoCommand(writer *RESPWriter, _ []RESP) {
	writer.WriteString("OK")
}

func fetchMap(key string, setnx ...bool) (Map, error) {
	return fetch(key, func() Map { return hash.NewMap() }, setnx...)
}

func fetchList(key string, setnx ...bool) (List, error) {
	return fetch(key, func() List { return list.New() }, setnx...)
}

func fetchSet(key string, setnx ...bool) (Set, error) {
	return fetch(key, func() Set { return hash.NewSet() }, setnx...)
}

func fetchZSet(key string, setnx ...bool) (ZSet, error) {
	return fetch(key, func() ZSet { return zset.NewZSet() }, setnx...)
}

func fetch[T any](key string, new func() T, setnx ...bool) (v T, err error) {
	item, ok := db.dict.Get(key)
	if ok {
		v, ok := item.(T)
		if ok {
			return v, nil
		}
		return v, ErrWrongType
	}
	v = new()
	if len(setnx) > 0 && setnx[0] {
		db.dict.Set(key, v)
	}
	return v, nil
}
