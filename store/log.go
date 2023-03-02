package store

import (
	"log"
	"os"
)

const (
	OP_SET byte = iota + 1
	OP_SET_WITH_TTL
	OP_DELETE
	OP_PERSIST
)

func NewLogger(path string) *log.Logger {
	writer, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	return log.New(writer, "", 0)
}
