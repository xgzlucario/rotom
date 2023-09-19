package base

import (
	"errors"
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrKeyIsEmpty  = errors.New("key is empty")

	ErrOutOfBounds = errors.New("index out of bounds")

	ErrFieldNotFound = errors.New("field not found")

	ErrWrongType     = errors.New("wrong data type")
	ErrWrongBitValue = errors.New("wrong bit value")

	ErrUnSupportDataType    = errors.New("unsupport data type")
	ErrUnknownOperationType = errors.New("unknown operation type")

	ErrNotString   = errors.New("value is not string")
	ErrNotNumberic = errors.New("value is not numberic")

	ErrParseRecordLine = errors.New("parse record line error, db file is broken")
	ErrInvalidResponse = errors.New("invalid response")

	ErrDatabaseClosed = errors.New("database closed")

	ErrEmptyList = errors.New("list is empty")
)
