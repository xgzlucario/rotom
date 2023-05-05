package base

import (
	"errors"
)

var (
	ErrOutOfBounds = errors.New("index out of bounds")

	ErrKeyNotFound = errors.New("key not found")

	ErrWrongType = errors.New("wrong type")

	ErrUnSupportType = errors.New("unsupport data type")
)
