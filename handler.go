package main

import (
	"fmt"

	"github.com/xgzlucario/rotom/structx"
)

var (
	// cmdTable is the list of all available commands.
	cmdTable []*Command = []*Command{
		{"ping", pingCommand, 0, false},
		{"set", setCommand, 2, true},
		{"get", getCommand, 1, false},
		{"hset", hsetCommand, 3, true},
		{"hget", hgetCommand, 2, false},
		{"hdel", hdelCommand, 2, true},
		{"hgetall", hgetallCommand, 1, false},
		{"rpush", rpushCommand, 2, true},
		{"lpush", lpushCommand, 2, true},
		{"lrange", lrangeCommand, 3, false},
	}
)

func pingCommand(_ []Value) Value {
	return Value{typ: STRING, raw: []byte("PONG")}
}

func setCommand(args []Value) Value {
	key := args[0].ToString()
	value := args[1].ToBytes()
	db.strs.Set(key, value)
	return ValueOK
}

func getCommand(args []Value) Value {
	key := args[0].ToString()

	value, _, ok := db.strs.Get(key)
	if ok {
		return newBulkValue(value)
	}
	// check extra maps
	_, ok = db.extras[key]
	if ok {
		return newErrValue(ErrWrongType)
	}
	return ValueNull
}

func hsetCommand(args []Value) Value {
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
		value := args[i+1].ToBytes()
		if hmap.Set(key, value) {
			newFields++
		}
	}
	return newIntegerValue(newFields)
}

func hgetCommand(args []Value) Value {
	hash := args[0].ToString()
	key := args[1].ToString()

	hmap, err := fetchMap(hash)
	if err != nil {
		return newErrValue(ErrWrongType)
	}
	value, _, ok := hmap.Get(key)
	if !ok {
		return ValueNull
	}
	return newBulkValue(value)
}

func hdelCommand(args []Value) Value {
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

func hgetallCommand(args []Value) Value {
	hash := args[0].ToString()

	hmap, err := fetchMap(hash)
	if err != nil {
		return newErrValue(err)
	}

	res := make([]Value, 0, 8)
	hmap.Scan(func(key, value []byte) {
		res = append(res, newBulkValue(key))
		res = append(res, newBulkValue(value))
	})
	return newArrayValue(res)
}

func pushInternal(args []Value, isDirectLeft bool) Value {
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

func lpushCommand(args []Value) Value {
	return pushInternal(args, true)
}

func rpushCommand(args []Value) Value {
	return pushInternal(args, false)
}

func lrangeCommand(args []Value) Value {
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

func fetchMap(key string, setnx ...bool) (Map, error) {
	return fetch(key, func() Map { return structx.NewMap() }, setnx...)
}

func fetchList(key string, setnx ...bool) (List, error) {
	return fetch(key, func() List { return structx.NewList() }, setnx...)
}

func fetch[T any](key string, new func() T, setnx ...bool) (v T, err error) {
	item, ok := db.extras[key]
	if ok {
		v, ok := item.(T)
		if ok {
			return v, nil
		}
		return v, fmt.Errorf("wrong type assert: %T->%T", item, v)
	}

	v = new()
	if len(setnx) > 0 && setnx[0] {
		db.extras[key] = v
	}
	return v, nil
}
