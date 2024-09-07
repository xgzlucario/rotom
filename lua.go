package main

import (
	"github.com/xgzlucario/rotom/internal/dict"
	lua "github.com/yuin/gopher-lua"
)

func OpenRedis(L *lua.LState) int {
	mod := L.RegisterModule("redis", map[string]lua.LGFunction{
		"call": libCall,
	})
	L.Push(mod)
	return 1
}

func libCall(L *lua.LState) int {
	fn := L.ToString(1)
	switch fn {
	case "set":
		return libSet(L)
	case "get":
		return libGet(L)
	}
	return -1
}

func libSet(L *lua.LState) int {
	key := L.ToString(2)
	value := L.ToString(3)
	db.dict.Set(key, []byte(value))
	L.Push(lua.LString("OK"))
	return 1
}

func libGet(L *lua.LState) int {
	key := L.ToString(2)
	value, ttl := db.dict.Get(key)
	if ttl != dict.KEY_NOT_EXIST {
		L.Push(lua.LString(value.([]byte)))
	} else {
		L.Push(lua.LNil)
	}
	return 1
}
