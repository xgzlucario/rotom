package main

import (
	"github.com/xgzlucario/rotom/internal/hash"
	"github.com/xgzlucario/rotom/internal/iface"
	"github.com/xgzlucario/rotom/internal/list"
)

type ObjectType byte

const (
	TypeUnknown ObjectType = iota
	TypeString  ObjectType = iota
	TypeInteger
	TypeMap
	TypeZipMap
	TypeSet
	TypeZipSet
	TypeList
	TypeZSet
)

const (
	TTL_FOREVER   = -1
	KEY_NOT_EXIST = -2
)

// type2c is objectType to new encoder.
var type2c = map[ObjectType]func() iface.Encoder{
	TypeMap:    func() iface.Encoder { return hash.NewMap() },
	TypeZipMap: func() iface.Encoder { return hash.NewZipMap() },
	TypeSet:    func() iface.Encoder { return hash.NewSet() },
	TypeZipSet: func() iface.Encoder { return hash.NewZipSet() },
	TypeList:   func() iface.Encoder { return list.New() },
}
