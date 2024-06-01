package main

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	cache "github.com/xgzlucario/GigaCache"
	"github.com/xgzlucario/rotom/structx"
)

var (
	ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

func pingCommand(_ []Value) Value {
	return Value{typ: STRING, str: "PONG"}
}

func setCommand(args []Value) Value {
	key := args[0].bulk
	value := args[1].bulk
	var ttl int64

	for i, arg := range args[2:] {
		switch b2s(arg.bulk) {
		case "NX":
		case "PX":
		case "EX":
			if len(args) > i+3 {
				seconds, _ := strconv.Atoi(b2s(args[i+3].bulk))
				ttl = cache.GetNanoSec() + int64(seconds)*int64(time.Second)
			} else {
				return NewErrValue(ErrWrongArgs("set"))
			}
		}
	}
	db.strs.SetTx(b2s(key), value, ttl)
	return ValueOK
}

func getCommand(args []Value) Value {
	key := args[0].bulk

	value, _, ok := db.strs.Get(b2s(key))
	if ok {
		return NewBulkValue(value)
	}
	// check extra maps
	_, ok = db.extras[b2s(key)]
	if ok {
		return NewErrValue(ErrWrongType)
	}
	return ValueNull
}

func hsetCommand(args []Value) Value {
	hash := b2s(args[0].bulk)
	key := b2s(args[1].bulk)
	value := args[2].bulk

	m, err := fetchMap(hash, true)
	if err != nil {
		return NewErrValue(err)
	}
	m.Set(key, value)
	return ValueOK
}

func hgetCommand(args []Value) Value {
	hash := args[0].bulk
	key := args[1].bulk

	m, err := fetchMap(b2s(hash))
	if err != nil {
		return NewErrValue(ErrWrongType)
	}
	value, _, ok := m.Get(b2s(key))
	if !ok {
		return ValueNull
	}
	return NewBulkValue(value)
}

func hdelCommand(args []Value) Value {
	hash := args[0].bulk
	keys := args[1:]

	m, err := fetchMap(b2s(hash))
	if err != nil {
		return NewErrValue(err)
	}
	var success int
	for _, v := range keys {
		if m.Remove(b2s(v.bulk)) {
			success++
		}
	}
	return NewIntegerValue(success)
}

func hgetallCommand(args []Value) Value {
	hash := args[0].bulk

	m, err := fetchMap(b2s(hash))
	if err != nil {
		return NewErrValue(err)
	}

	res := Value{typ: ARRAY}
	m.Scan(func(key, value []byte) {
		res.array = append(res.array, Value{typ: BULK, bulk: key})
		res.array = append(res.array, Value{typ: BULK, bulk: value})
	})
	return res
}

func fetchMap(key string, setnx ...bool) (Map, error) {
	return fetch(key, func() Map { return structx.NewMap() }, setnx...)
}

// func fetchSet(key string, setnx ...bool) (Set, error) {
// 	return fetch(key, func() Set { return structx.NewSet() }, setnx...)
// }

// func fetchList(key string, setnx ...bool) (List, error) {
// 	return fetch(key, func() List { return structx.NewList() }, setnx...)
// }

// func fetchBitMap(key string, setnx ...bool) (BitMap, error) {
// 	return fetch(key, func() BitMap { return structx.NewBitmap() }, setnx...)
// }

// func fetchZSet(key string, setnx ...bool) (ZSet, error) {
// 	return fetch(key, func() ZSet { return structx.NewZSet() }, setnx...)
// }

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