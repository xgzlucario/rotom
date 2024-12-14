package main

import (
	"github.com/redis/go-redis/v9"
	"github.com/xgzlucario/rotom/internal/hash"
	"github.com/xgzlucario/rotom/internal/iface"
	"github.com/xgzlucario/rotom/internal/list"
	"github.com/xgzlucario/rotom/internal/zset"
)

type ObjectType byte

const (
	TypeUnknown ObjectType = iota
	TypeString
	TypeInteger
	TypeMap
	TypeSet
	TypeZipSet
	TypeList
	TypeZSet
	TypeZipZSet
)

const (
	KeepTTL     int64 = redis.KeepTTL
	KeyNotExist int64 = -2
)

const (
	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB
)

var type2c = map[ObjectType]func() iface.Encoder{
	TypeMap:     func() iface.Encoder { return hash.New() },
	TypeSet:     func() iface.Encoder { return hash.NewSet() },
	TypeZipSet:  func() iface.Encoder { return hash.NewZipSet() },
	TypeList:    func() iface.Encoder { return list.New() },
	TypeZSet:    func() iface.Encoder { return zset.New() },
	TypeZipZSet: func() iface.Encoder { return zset.NewZipZSet() },
}
