package main

import (
	"errors"
	"fmt"
)

var (
	ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

	ErrUnknownType = errors.New("ERR unknown value type")

	ErrParseInteger = errors.New("ERR value is not an integer or out of range")

	ErrCRLFNotFound = errors.New("ERR CRLF not found in line")
)

func ErrWrongNumberArgs(cmd string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd)
}

func ErrUnknownCommand(cmd string) error {
	return fmt.Errorf("ERR unknown command '%s'", cmd)
}
