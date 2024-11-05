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
)

const (
	TTL_FOREVER   = -1
	KEY_NOT_EXIST = -2
)

type Object interface {
	GetType() ObjectType
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
}
