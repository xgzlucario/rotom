package base

import (
	"fmt"
	"reflect"
)

func ErrOutOfBounds(index int) error {
	return fmt.Errorf("error: index[%d] out of bounds", index)
}

func ErrKeyNotFound(key any) error {
	return fmt.Errorf("error: key[%v] not found", key)
}

func ErrType(t any) error {
	return fmt.Errorf("error: type is not %v", reflect.TypeOf(t))
}
