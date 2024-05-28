package main

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/xgzlucario/rotom/structx"
)

var (
	ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

func pingCommand(c *RotomClient) {
	c.addReplyStr("PONG")
}

func setCommand(c *RotomClient) {
	key := c.args[0].bulk
	value := c.args[1].bulk
	var seconds int

	for i, arg := range c.args[2:] {
		switch b2s(arg.bulk) {
		case "NX":
			// TODO

		case "EX":
			if len(c.args) > i+3 {
				seconds, _ = strconv.Atoi(b2s(c.args[i+3].bulk))
			} else {
				c.addReplyWrongNumberArgs()
				return
			}
		}
	}

	db.strs.SetEx(b2s(key), value, time.Second*time.Duration(seconds))
	db.aof.Write(Value{typ: TypeArray, array: c.rawargs})
	c.addReplyStr("OK")
}

func getCommand(c *RotomClient) {
	key := c.args[0].bulk

	value, _, ok := db.strs.Get(b2s(key))
	if ok {
		c.addReplyBulk(value)
		return
	}
	// check extra maps
	_, ok = db.extras[b2s(key)]
	if ok {
		c.addReplyError(ErrWrongType)
		return
	}
	c.addReplyNull()
}

func expireCommand(c *RotomClient) {
	key := c.args[0].bulk
	seconds := c.args[1].num

	db.strs.SetTTL(b2s(key), seconds*1e9)
	db.aof.Write(Value{typ: TypeArray, array: c.rawargs})
	c.addReplyStr("OK")
}

func hsetCommand(c *RotomClient) {
	hash := b2s(c.args[0].bulk)
	key := b2s(c.args[1].bulk)
	value := c.args[2].bulk

	m, err := fetchMap(hash, true)
	if err != nil {
		c.addReplyError(err)
		return
	}
	m.Set(key, value)
	db.aof.Write(Value{typ: TypeArray, array: c.rawargs})
	c.addReplyStr("OK")
}

func hgetCommand(c *RotomClient) {
	hash := c.args[0].bulk
	key := c.args[1].bulk

	m, err := fetchMap(b2s(hash))
	if err != nil {
		c.addReplyError(err)
		return
	}
	value, _, ok := m.Get(b2s(key))
	if !ok {
		c.addReplyNull()
		return
	}
	c.addReplyBulk(value)
}

func hdelCommand(c *RotomClient) {
	hash := c.args[0].bulk
	keys := c.args[1:]

	m, err := fetchMap(b2s(hash))
	if err != nil {
		c.addReplyError(err)
		return
	}
	var success int64
	for _, v := range keys {
		if m.Remove(b2s(v.bulk)) {
			success++
		}
	}
	db.aof.Write(Value{typ: TypeArray, array: c.rawargs})
	c.addReplyInteger(success)
}

func hgetallCommand(c *RotomClient) {
	hash := c.args[0].bulk

	m, err := fetchMap(b2s(hash))
	if err != nil {
		c.addReplyError(err)
		return
	}
	var res [][]byte
	m.Scan(func(key, value []byte) {
		res = append(res, key)
		res = append(res, value)
	})
	c.addReplyArrayBulk(res)
}

func fetchMap(key string, setnx ...bool) (Map, error) {
	return fetch(key, func() Map { return structx.NewMap() }, setnx...)
}

func fetchSet(key string, setnx ...bool) (Set, error) {
	return fetch(key, func() Set { return structx.NewSet() }, setnx...)
}

func fetchList(key string, setnx ...bool) (List, error) {
	return fetch(key, func() List { return structx.NewList() }, setnx...)
}

func fetchBitMap(key string, setnx ...bool) (BitMap, error) {
	return fetch(key, func() BitMap { return structx.NewBitmap() }, setnx...)
}

func fetchZSet(key string, setnx ...bool) (ZSet, error) {
	return fetch(key, func() ZSet { return structx.NewZSet() }, setnx...)
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
