package base

import (
	"errors"
)

var (
	ErrKeyNotFound = errors.New("key not found")

	ErrFieldNotFound = errors.New("field not found")

	ErrTypeAssert = errors.New("type assert error")

	ErrOutOfBounds = errors.New("index out of bounds")

	ErrWrongType = errors.New("wrong data type")

	ErrWrongBitValue = errors.New("wrong bit value")

	ErrUnSupportDataType = errors.New("unsupport data type")

	ErrUnknownOperationType = errors.New("unknown operation type")

	ErrNotString = errors.New("value is not string")

	ErrNotNumberic = errors.New("value is not numberic")

	ErrCheckSum = errors.New("crc checksum error, record line is invalid")

	ErrInvalidArgs = errors.New("invalid args")

	ErrInvalidResponse = errors.New("invalid response")

	ErrDatabaseClosed = errors.New("database closed")

	ErrUnSupportOperation = errors.New("unsupport operation")

	ErrIndexOutOfRange = errors.New("index out of range")

	ErrEmptyList = errors.New("list is empty")
)
