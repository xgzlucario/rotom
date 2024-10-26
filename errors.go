package main

import (
	"errors"
)

var (
	errWrongType      = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	errParseInteger   = errors.New("ERR value is not an integer or out of range")
	errCRLFNotFound   = errors.New("ERR CRLF not found in line")
	errWrongArguments = errors.New("ERR wrong number of arguments")
	errUnknownCommand = errors.New("ERR unknown command")
	errSyntax         = errors.New("ERR syntax error")
)
