package main

import (
	"github.com/redis/go-redis/v9"
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
