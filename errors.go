package main

import (
	"errors"
	"fmt"
)

var (
	ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

	ErrUnknownType = errors.New("ERR unknown value type")
)

func ErrWrongArgs(cmd string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd)
}
