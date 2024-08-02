package main

import (
	"errors"
)

var (
	errWrongType        = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	errParseInteger     = errors.New("ERR value is not an integer or out of range")
	errCRLFNotFound     = errors.New("ERR CRLF not found in line")
	errInvalidArguments = errors.New("ERR invalid number of arguments")
	errUnknownCommand   = errors.New("ERR unknown command")
	errOOM              = errors.New("ERR command not allowed when out of memory")
	// errSyntax           = errors.New("ERR syntax error")
)
