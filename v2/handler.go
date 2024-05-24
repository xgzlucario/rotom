package main

import (
	"fmt"

	"github.com/xgzlucario/rotom/structx"
)

var (
	RespOK   = []byte("OK")
	RespPong = []byte("PONG")
)

func pingCommand(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: TypeString, str: RespPong}
	}
	return Value{typ: TypeString, str: args[0].bulk}
}

func setCommand(args []Value) Value {
	key := args[0].bulk
	value := args[1].bulk

	db.strs.Set(b2s(key), value)

	return Value{typ: TypeString, str: RespOK}
}

func getCommand(args []Value) Value {
	key := args[0].bulk

	value, _, ok := db.strs.Get(b2s(key))
	if !ok {
		return Value{typ: TypeNull}
	}

	return Value{typ: TypeBulk, bulk: value}
}

func hsetCommand(args []Value) Value {
	hash := b2s(args[0].bulk)
	key := b2s(args[1].bulk)
	value := args[2].bulk

	m, err := fetchMap(hash, true)
	if err != nil {
		return ErrValue(err.Error())
	}
	m.Set(key, value)

	return Value{typ: TypeString, str: RespOK}
}

func hgetCommand(args []Value) Value {
	hash := args[0].bulk
	key := args[1].bulk

	m, err := fetchMap(b2s(hash))
	if err != nil {
		return ErrValue(err.Error())
	}

	value, _, ok := m.Get(b2s(key))
	if !ok {
		return Value{typ: TypeNull}
	}

	return Value{typ: TypeBulk, bulk: []byte(value)}
}

func hdelCommand(args []Value) Value {
	hash := args[0].bulk
	keys := args[1:]

	m, err := fetchMap(b2s(hash))
	if err != nil {
		return ErrValue(err.Error())
	}

	var success int64
	for _, v := range keys {
		if m.Remove(b2s(v.bulk)) {
			success++
		}
	}

	return Value{typ: TypeInteger, num: success}
}

func hgetallCommand(args []Value) Value {
	hash := args[0].bulk

	m, err := fetchMap(b2s(hash))
	if err != nil {
		return ErrValue(err.Error())
	}

	var values []Value
	m.Scan(func(key, value []byte) {
		values = append(values, Value{typ: TypeBulk, bulk: key})
		values = append(values, Value{typ: TypeBulk, bulk: value})
	})

	return Value{typ: TypeArray, array: values}
}

func fetchMap(key string, setnx ...bool) (Map, error) {
	return fetch(key, func() Map {
		return structx.NewMap()
	}, setnx...)
}

func fetchSet(key string, setnx ...bool) (Set, error) {
	return fetch(key, func() Set {
		return structx.NewSet()
	}, setnx...)
}

func fetchList(key string, setnx ...bool) (List, error) {
	return fetch(key, func() List {
		return structx.NewList()
	}, setnx...)
}

func fetchBitMap(key string, setnx ...bool) (BitMap, error) {
	return fetch(key, func() BitMap {
		return structx.NewBitmap()
	}, setnx...)
}

func fetchZSet(key string, setnx ...bool) (ZSet, error) {
	return fetch(key, func() ZSet {
		return structx.NewZSet()
	}, setnx...)
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
