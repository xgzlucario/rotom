package main

import (
	"github.com/xgzlucario/rotom/internal/dict"
	lua "github.com/yuin/gopher-lua"
)

func luaCall(L *lua.LState) int {
	fn := L.ToString(1)
	keys := L.GetGlobal("KEYS").(*lua.LTable)
	switch fn {
	case "set":
		return luaSet(L, keys)
	case "get":
		return luaGet(L, keys)
	}
	return -1
}

func luaSet(L *lua.LState, keys *lua.LTable) int {
	key := keys.RawGetInt(1).String()
	value := keys.RawGetInt(2).String()
	db.dict.Set(key, []byte(value))
	L.Push(lua.LString("OK"))
	return 1
}

func luaGet(L *lua.LState, keys *lua.LTable) int {
	key := keys.RawGetInt(1).String()
	value, ttl := db.dict.Get(key)
	if ttl != dict.KEY_NOT_EXIST {
		L.Push(lua.LString(value.([]byte)))
	} else {
		L.Push(lua.LNil)
	}
	return 1
}
