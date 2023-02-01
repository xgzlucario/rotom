package base

import (
	"fmt"
	"reflect"
)

func ErrOutOfBounds(index int) error {
	return fmt.Errorf("index[%d] out of bounds", index)
}

func ErrKeyNotFound(key any) error {
	return fmt.Errorf("key[%v] not found", key)
}

func ErrType(t any) error {
	return fmt.Errorf("type is not %v", reflect.TypeOf(t))
}
