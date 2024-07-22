package main

import (
	"strconv"

	"github.com/xgzlucario/rotom/internal/dict"
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
	{"flushdb", flushdbCommand, 0, true},

	// TODO
	{"mset", todoCommand, 0, false},
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
	db.dict.Set(key, dict.TypeString, value)
	writer.WriteString("OK")
}

func incrCommand(writer *RESPWriter, args []RESP) {
	key := args[0].ToString()

	object, ok := db.dict.Get(key)
	if !ok {
		db.dict.Set(key, dict.TypeInt, 1)
		writer.WriteInteger(1)
		return
	}

	switch object.Type() {
	case dict.TypeInt:
		num := object.Data().(int) + 1
		object.SetData(num)
		writer.WriteInteger(num)

	case dict.TypeString:
		bytes := object.Data().([]byte)
		num, err := RESP(bytes).ToInt()
		if err != nil {
			writer.WriteError(errParseInteger)
			return
		}
		num++
		object.SetData([]byte(strconv.Itoa(num)))
		writer.WriteInteger(num)

	default:
		writer.WriteError(errWrongType)
	}
}

func getCommand(writer *RESPWriter, args []RESP) {
	key := args[0].ToStringUnsafe()

	object, ok := db.dict.Get(key)
	if !ok {
		writer.WriteNull()
		return
	}

	switch object.Type() {
	case dict.TypeInt:
		num := object.Data().(int)
		writer.WriteBulkString(strconv.Itoa(num))

	case dict.TypeString:
		bytes := object.Data().([]byte)
		writer.WriteBulk(bytes)

	default:
		writer.WriteError(errWrongType)
	}
}

func hsetCommand(writer *RESPWriter, args []RESP) {
	hash := args[0].ToString()
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

func flushdbCommand(writer *RESPWriter, _ []RESP) {
	db.dict = dict.New()
	writer.WriteString("OK")
}

// TODO
func todoCommand(writer *RESPWriter, _ []RESP) {
	writer.WriteString("OK")
}

func fetchMap(key string, setnx ...bool) (Map, error) {
	return fetch(key, dict.TypeZipMap, func() Map { return hash.NewZipMap() }, setnx...)
}

func fetchList(key string, setnx ...bool) (List, error) {
	return fetch(key, dict.TypeList, func() List { return list.New() }, setnx...)
}

func fetchSet(key string, setnx ...bool) (Set, error) {
	return fetch(key, dict.TypeZipSet, func() Set { return hash.NewZipSet() }, setnx...)
}

func fetchZSet(key string, setnx ...bool) (ZSet, error) {
	return fetch(key, dict.TypeZSet, func() ZSet { return zset.NewZSet() }, setnx...)
}

func fetch[T any](key string, typ dict.Type, new func() T, setnx ...bool) (v T, err error) {
	object, ok := db.dict.Get(key)
	if ok {
		if object.Type() != typ {
			return v, errWrongType
		}
		v := object.Data().(T)
		return v, nil
	}

	v = new()
	if len(setnx) > 0 && setnx[0] {
		db.dict.Set(key, typ, v)
	}
	return v, nil
}
