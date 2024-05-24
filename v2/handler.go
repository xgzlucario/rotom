package main

import (
	cache "github.com/xgzlucario/GigaCache"
)

// Handlers maps command strings to their respective handler functions.
var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetall,
}

var (
	RespOK   = []byte("OK")
	RespPong = []byte("PONG")
)

var SETs *cache.GigaCache
var HSETs = map[string]map[string]string{}

func init() {
	SETs = cache.New(cache.DefaultOptions)
}

func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: TypeString, str: RespPong}
	}
	return Value{typ: TypeString, str: args[0].bulk}
}

func set(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: TypeError, str: []byte("ERR wrong number of arguments for 'set' command")}
	}

	key := args[0].bulk
	value := args[1].bulk

	SETs.Set(b2s(key), value)

	return Value{typ: TypeString, str: RespOK}
}

func get(args []Value) Value {
	if len(args) != 1 {
		return ErrValue("ERR wrong number of arguments for 'get' command")
	}

	key := args[0].bulk

	value, _, ok := SETs.Get(b2s(key))
	if !ok {
		return Value{typ: TypeNull}
	}

	return Value{typ: TypeBulk, bulk: value}
}

func hset(args []Value) Value {
	if len(args) != 3 {
		return ErrValue("ERR wrong number of arguments for 'hset' command")
	}
	hash := b2s(args[0].bulk)
	key := b2s(args[1].bulk)
	value := b2s(args[2].bulk)

	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value

	return Value{typ: TypeString, str: RespOK}
}

func hget(args []Value) Value {
	if len(args) != 2 {
		return ErrValue("ERR wrong number of arguments for 'hget' command")
	}

	hash := args[0].bulk
	key := args[1].bulk

	value, ok := HSETs[b2s(hash)][b2s(key)]
	if !ok {
		return Value{typ: TypeNull}
	}

	return Value{typ: TypeBulk, bulk: []byte(value)}
}

func hgetall(args []Value) Value {
	if len(args) != 1 {
		return ErrValue("ERR wrong number of arguments for 'hgetall' command")
	}

	hash := args[0].bulk

	value, ok := HSETs[b2s(hash)]
	if !ok {
		return Value{typ: TypeNull}
	}

	var values []Value
	for k, v := range value {
		values = append(values, Value{typ: TypeBulk, bulk: []byte(k)})
		values = append(values, Value{typ: TypeBulk, bulk: []byte(v)})
	}

	return Value{typ: TypeArray, array: values}
}
