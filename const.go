package main

type ObjectType byte

const (
	TypeString ObjectType = iota
	TypeInteger
	TypeMap
	TypeZipMap
	TypeSet
	TypeZipSet
	TypeList
	TypeZSet
	TypeUnknown = 255
)

const (
	TTL_FOREVER   = -1
	KEY_NOT_EXIST = -2
)
