package structx

import (
	"fmt"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type JsonX []byte

func NewJsonX(src string) JsonX {
	return JsonX(src)
}

func (x *JsonX) GetJson(path string) gjson.Result {
	return gjson.GetBytes(*x, path)
}

func (x *JsonX) SetJson(path string, value any, optimistic ...bool) (err error) {
	opt := &sjson.Options{
		Optimistic:     len(optimistic) > 0 && optimistic[0],
		ReplaceInPlace: true,
	}
	*x, err = sjson.SetBytesOptions(*x, path, value, opt)
	return
}

func (x *JsonX) SetRawJson(path string, value []byte, optimistic ...bool) (err error) {
	opt := &sjson.Options{
		Optimistic:     len(optimistic) > 0 && optimistic[0],
		ReplaceInPlace: true,
	}
	*x, err = sjson.SetRawBytesOptions(*x, path, value, opt)
	return
}

func (x JsonX) Print() {
	fmt.Println(string(x))
}
