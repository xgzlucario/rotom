package main

import (
	"sync"

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

type RotomServer struct {
	strings *cache.GigaCache
	types   map[string]any
}

func init() {
	SETs = cache.New(cache.DefaultOptions)
}

// ping responds to the PING command, optionally echoing back any additional argument.
func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}
	return Value{typ: "string", str: args[0].bulk}
}

// SETs stores key-value pairs for the SET and GET commands.
var SETs *cache.GigaCache

// set handles the SET command to store a key-value pair.
func set(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'set' command"}
	}

	key := args[0].bulk
	value := args[1].bulk
	SETs.Set(key, []byte(value))

	return Value{typ: "string", str: "OK"}
}

// get retrieves a value for a key using the GET command.
func get(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'get' command"}
	}

	key := args[0].bulk

	value, _, ok := SETs.Get(key)
	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: string(value)}
}

// HSETs stores nested hash map structures for the HSET, HGET, and HGETALL commands.
var HSETs = map[string]map[string]string{}
var HSETsMu sync.RWMutex // Protects HSETs

// hset handles the HSET command to store a key-value pair in a hash map.
func hset(args []Value) Value {
	if len(args) != 3 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hset' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk
	value := args[2].bulk

	HSETsMu.Lock()
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value
	HSETsMu.Unlock()

	return Value{typ: "string", str: "OK"}
}

// hget retrieves a value for a key within a hash using the HGET command.
func hget(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hget' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash][key]
	HSETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}
}

// hgetall retrieves all key-value pairs within a hash using the HGETALL command.
func hgetall(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hgetall' command"}
	}

	hash := args[0].bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash]
	HSETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	var values []Value
	for k, v := range value {
		values = append(values, Value{typ: "bulk", bulk: k})
		values = append(values, Value{typ: "bulk", bulk: v})
	}

	return Value{typ: "array", array: values}
}
