package main

import (
	"github.com/xgzlucario/rotom/internal/dict"
	lua "github.com/yuin/gopher-lua"
)

func luaCall(L *lua.LState) int {
	fn := L.ToString(1)
	var keyTable, argvTable *lua.LTable
	if t := L.GetGlobal("KEYS"); t.Type() == lua.LTTable {
		keyTable = t.(*lua.LTable)
	}
	if t := L.GetGlobal("ARGV"); t.Type() != lua.LTNil {
		argvTable = t.(*lua.LTable)
	}
	switch fn {
	case "set":
		return luaSet(L, keyTable, argvTable)
	case "get":
		return luaGet(L, keyTable, argvTable)
	}
	return 0
}

func luaSet(L *lua.LState, keyTable, _ *lua.LTable) int {
	key := keyTable.RawGetInt(1).String()
	value := keyTable.RawGetInt(2).String()
	db.dict.Set(key, []byte(value))
	L.Push(lua.LString("OK"))
	return 1
}

func luaGet(L *lua.LState, keyTable, _ *lua.LTable) int {
	key := keyTable.RawGetInt(1).String()
	value, ttl := db.dict.Get(key)
	if ttl != dict.KEY_NOT_EXIST {
		L.Push(lua.LString(value.([]byte)))
	} else {
		L.Push(lua.LNil)
	}
	return 1
}
