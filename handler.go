package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/xgzlucario/rotom/structx"
)

func pingCommand(_ []Value) Value {
	return Value{typ: STRING, str: "PONG"}
}

func setCommand(args []Value) Value {
	key := args[0].bulk
	value := args[1].bulk
	exargs := args[2:]
	var duration time.Duration

	for i, arg := range exargs {
		switch b2s(arg.bulk) {
		case "NX", "nx":
		case "PX", "px":
		case "EX", "ex":
			if len(exargs) > i+1 {
				seconds, err := strconv.Atoi(b2s(exargs[i+1].bulk))
				if err != nil {
					return newErrValue(ErrParseInteger)
				}
				duration = time.Duration(seconds)
			} else {
				return newErrValue(ErrWrongNumberArgs("set"))
			}
		}
	}
	db.strs.SetEx(b2s(key), value, duration)
	return ValueOK
}

func getCommand(args []Value) Value {
	key := b2s(args[0].bulk)

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
	hash := args[0].bulk

	// check arguments number
	exargs := args[1:]
	if len(exargs) == 0 || len(exargs)%2 == 1 {
		return newErrValue(ErrWrongNumberArgs("hset"))
	}

	hmap, err := fetchMap(hash, true)
	if err != nil {
		return newErrValue(err)
	}

	var newFields int
	for i := 0; i < len(exargs); i += 2 {
		key := exargs[i].bulk
		value := exargs[i+1].bulk
		if hmap.Set(b2s(key), value) {
			newFields++
		}
	}
	return newIntegerValue(newFields)
}

func hgetCommand(args []Value) Value {
	hash := args[0].bulk
	key := args[1].bulk

	hmap, err := fetchMap(hash)
	if err != nil {
		return newErrValue(ErrWrongType)
	}
	value, _, ok := hmap.Get(b2s(key))
	if !ok {
		return ValueNull
	}
	return newBulkValue(value)
}

func hdelCommand(args []Value) Value {
	hash := args[0].bulk
	keys := args[1:]

	hmap, err := fetchMap(hash)
	if err != nil {
		return newErrValue(err)
	}
	var success int
	for _, v := range keys {
		if hmap.Remove(b2s(v.bulk)) {
			success++
		}
	}
	return newIntegerValue(success)
}

func hgetallCommand(args []Value) Value {
	hash := args[0].bulk

	hmap, err := fetchMap(hash)
	if err != nil {
		return newErrValue(err)
	}

	res := make([]Value, 0, 8)
	hmap.Scan(func(key, value []byte) {
		res = append(res, Value{typ: BULK, bulk: key})
		res = append(res, Value{typ: BULK, bulk: value})
	})
	return newArrayValue(res)
}

func fetchMap(key []byte, setnx ...bool) (Map, error) {
	return fetch(key, func() Map { return structx.NewMap() }, setnx...)
}

func fetch[T any](key []byte, new func() T, setnx ...bool) (v T, err error) {
	item, ok := db.extras[b2s(key)]
	if ok {
		v, ok := item.(T)
		if ok {
			return v, nil
		}
		return v, fmt.Errorf("wrong type assert: %T->%T", item, v)
	}

	v = new()
	if len(setnx) > 0 && setnx[0] {
		// here NEED to use copy of key
		db.extras[string(key)] = v
	}
	return v, nil
}
