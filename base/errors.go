package base

import (
	"errors"
)

var (
	ErrOutOfBounds = errors.New("index out of bounds")

	ErrKeyNotFound = errors.New("key not found")

	ErrKeyIsEmpty = errors.New("key is empty")

	ErrWrongType = errors.New("wrong data type")

	ErrUnSupportDataType = errors.New("unsupport data type")

	ErrUnknownOperationType = errors.New("unknown operation type")

	ErrParseRecordLine = errors.New("parse record line error")
)
