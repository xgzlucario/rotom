package base

import (
	"errors"
	"time"
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

	ErrParseRecordLine = errors.New("parse record line error")
)

// Assert1 panic if err not nil
func Assert1(err error) {
	if err != nil {
		panic(err)
	}
}

// Go start a background worker
func Go(interval time.Duration, f func()) {
	go func() {
		for {
			time.Sleep(interval)
			f()
		}
	}()
}
