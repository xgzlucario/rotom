package main

import (
	"errors"
	"fmt"
)

var (
	errWrongType        = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	errParseInteger     = errors.New("ERR value is not an integer or out of range")
	errCRLFNotFound     = errors.New("ERR CRLF not found in line")
	errInvalidArguments = errors.New("ERR invalid number of arguments")
	errOOM              = errors.New("ERR command not allowed when out of memory")
)

func ErrUnknownCommand(cmd string) error {
	return fmt.Errorf("ERR unknown command '%s'", cmd)
}
