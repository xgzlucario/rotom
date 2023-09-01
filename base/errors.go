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

	ErrParseRecordLine = errors.New("parse record line error, maybe db file is broken")

	ErrDatabaseClosed = errors.New("database closed")

	// List
	ErrListEmpty = errors.New("list is empty")
)
