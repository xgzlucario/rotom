package main

import (
	"strconv"

	"github.com/xgzlucario/rotom/structx"
)

type Command struct {
	// name is lowercase letters command name.
	name string

	// handler is this command real database handler function.
	handler func([]Arg) Value

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
	{"spop", spopCommand, 1, true},
	{"zadd", zaddCommand, 3, true},
	{"ping", pingCommand, 0, false},
	{"hgetall", hgetallCommand, 1, false},
	{"lrange", lrangeCommand, 3, false},
	{"zpopmin", func(a []Arg) Value { return ValueOK }, 0, false}, // TODO
	{"xadd", func(a []Arg) Value { return ValueOK }, 0, false},    // TODO
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
	for i, lo := range lowerText {
		delta := lo - rune(str[i])
		if delta != 0 && delta != s {
			return false
		}
	}
	return true
}

func (cmd *Command) processCommand(args []Arg) Value {
	if len(args) < cmd.arity {
		return newErrValue(ErrWrongNumberArgs(cmd.name))
	}
	return cmd.handler(args)
}

func pingCommand(_ []Arg) Value {
	return Value{typ: STRING, raw: []byte("PONG")}
}

func setCommand(args []Arg) Value {
	key := args[0].ToString()
	value := args[1].ToBytes()
	db.strs.Set(key, value)
	return ValueOK
}

func msetCommand(args []Arg) Value {
	// check arguments number
	if len(args)%2 == 1 {
		return newErrValue(ErrWrongNumberArgs("hset"))
	}
	for i := 0; i < len(args); i += 2 {
		key := args[i].ToString()
		value := args[i+1].ToBytes()
		db.strs.Set(key, value)
	}
	return ValueOK
}

func incrCommand(args []Arg) Value {
	key := args[0].ToString()
	val, _, ok := db.strs.Get(key)
	if !ok {
		db.strs.Set(key, []byte("1"))
		return newIntegerValue(1)

	} else {
		num, err := strconv.Atoi(b2s(val))
		if err != nil {
			return newErrValue(ErrParseInteger)
		}
		num++
		db.strs.Set(key, []byte(strconv.Itoa(num)))
		return newIntegerValue(num)
	}
}

func getCommand(args []Arg) Value {
	key := args[0].ToStringUnsafe()

	value, _, ok := db.strs.Get(key)
	if ok {
		return newBulkValue(value)
	}
	// check extra maps
	_, ok = db.extras.Get(key)
	if ok {
		return newErrValue(ErrWrongType)
	}
	return ValueNull
}

func hsetCommand(args []Arg) Value {
	hash := args[0].ToString()
	args = args[1:]

	// check arguments number
	if len(args)%2 == 1 {
		return newErrValue(ErrWrongNumberArgs("hset"))
	}

	hmap, err := fetchMap(hash, true)
	if err != nil {
		return newErrValue(err)
	}

	var newFields int
	for i := 0; i < len(args); i += 2 {
		key := args[i].ToString()
		value := args[i+1].Clone()
		if hmap.Set(key, value) {
			newFields++
		}
	}
	return newIntegerValue(newFields)
}

func hgetCommand(args []Arg) Value {
	hash := args[0].ToStringUnsafe()
	key := args[1].ToStringUnsafe()

	hmap, err := fetchMap(hash)
	if err != nil {
		return newErrValue(ErrWrongType)
	}
	value, _ := hmap.Get(key)
	return newBulkValue(value)
}

func hdelCommand(args []Arg) Value {
	hash := args[0].ToString()
	keys := args[1:]

	hmap, err := fetchMap(hash)
	if err != nil {
		return newErrValue(err)
	}
	var success int
	for _, v := range keys {
		if hmap.Remove(v.ToString()) {
			success++
		}
	}
	return newIntegerValue(success)
}

func hgetallCommand(args []Arg) Value {
	hash := args[0].ToString()

	hmap, err := fetchMap(hash)
	if err != nil {
		return newErrValue(err)
	}

	res := make([]Value, 0, 8)
	hmap.Scan(func(key string, value []byte) {
		res = append(res, newBulkValue([]byte(key)))
		res = append(res, newBulkValue(value))
	})
	return newArrayValue(res)
}

func lpushCommand(args []Arg) Value {
	return pushInternal(args, true)
}

func rpushCommand(args []Arg) Value {
	return pushInternal(args, false)
}

func pushInternal(args []Arg, isDirectLeft bool) Value {
	key := args[0].ToString()

	ls, err := fetchList(key, true)
	if err != nil {
		return newErrValue(err)
	}
	if isDirectLeft {
		for _, arg := range args[1:] {
			ls.LPush(arg.ToString())
		}
	} else {
		for _, arg := range args[1:] {
			ls.RPush(arg.ToString())
		}
	}
	return newIntegerValue(ls.Size())
}

func lpopCommand(args []Arg) Value {
	return popInternal(args, true)
}

func rpopCommand(args []Arg) Value {
	return popInternal(args, false)
}

func popInternal(args []Arg, isDirectLeft bool) Value {
	key := args[0].ToString()

	ls, err := fetchList(key)
	if err != nil {
		return newErrValue(err)
	}

	var val string
	var ok bool
	if isDirectLeft {
		val, ok = ls.LPop()
	} else {
		val, ok = ls.RPop()
	}
	if ok {
		return newBulkValue([]byte(val))
	}
	return newBulkValue(nil)
}

func lrangeCommand(args []Arg) Value {
	key := args[0].ToString()
	start, err := args[1].ToInt()
	if err != nil {
		return newErrValue(err)
	}
	end, err := args[2].ToInt()
	if err != nil {
		return newErrValue(err)
	}

	ls, err := fetchList(key)
	if err != nil {
		return newErrValue(err)
	}

	var res []Value
	ls.Range(start, end, func(data []byte) (stop bool) {
		res = append(res, newBulkValue(data))
		return false
	})
	return newArrayValue(res)
}

func saddCommand(args []Arg) Value {
	key := args[0].ToString()
	args = args[1:]

	set, err := fetchSet(key, true)
	if err != nil {
		return newErrValue(err)
	}

	var newItems int
	for i := 0; i < len(args); i++ {
		if set.Add(args[i].ToString()) {
			newItems++
		}
	}
	return newIntegerValue(newItems)
}

func spopCommand(args []Arg) Value {
	key := args[0].ToString()

	set, err := fetchSet(key)
	if err != nil {
		return newErrValue(err)
	}

	item, ok := set.Pop()
	if ok {
		return newBulkValue([]byte(item))
	}
	return ValueNull
}

func zaddCommand(args []Arg) Value {
	key := args[0].ToString()
	args = args[1:]

	zset, err := fetchZSet(key, true)
	if err != nil {
		return newErrValue(err)
	}

	var newFields int
	for i := 0; i < len(args); i += 2 {
		score, err := args[i].ToInt()
		if err != nil {
			return newErrValue(err)
		}
		key := args[i+1].ToString()

		if zset.Set(key, int64(score)) {
			newFields++
		}
	}
	return newIntegerValue(newFields)
}

func fetchMap(key string, setnx ...bool) (Map, error) {
	return fetch(key, func() Map { return structx.NewMap() }, setnx...)
}

func fetchList(key string, setnx ...bool) (List, error) {
	return fetch(key, func() List { return structx.NewList() }, setnx...)
}

func fetchSet(key string, setnx ...bool) (Set, error) {
	return fetch(key, func() Set { return structx.NewSet() }, setnx...)
}

func fetchZSet(key string, setnx ...bool) (ZSet, error) {
	return fetch(key, func() ZSet { return structx.NewZSet() }, setnx...)
}

func fetch[T any](key string, new func() T, setnx ...bool) (v T, err error) {
	item, ok := db.extras.Get(key)
	if ok {
		v, ok := item.(T)
		if ok {
			return v, nil
		}
		return v, ErrWrongType
	}
	v = new()
	if len(setnx) > 0 && setnx[0] {
		db.extras.Put(key, v)
	}
	return v, nil
}
