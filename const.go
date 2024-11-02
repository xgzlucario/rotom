package main

type Object interface {
	GetType() ObjectType
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
}

type ObjectType byte

const (
	TypeUnknown ObjectType = iota
	TypeString
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
