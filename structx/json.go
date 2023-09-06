package structx

import (
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type Json struct {
	data []byte
}

// NewJson
func NewJson() *Json {
	return &Json{data: make([]byte, 0)}
}

// Get
func (j *Json) Get(path string) any {
	return gjson.GetBytes(j.data, path).Value()
}

// GetBytes
func (j *Json) GetBytes(path string) []any {
	res := gjson.GetManyBytes(j.data, path)
	arr := make([]any, 0, len(res))
	for _, v := range res {
		arr = append(arr, v.Value())
	}
	return arr
}

// SetBytes
func (j *Json) SetBytes(path string, val []byte) (err error) {
	j.data, err = sjson.SetBytesOptions(j.data, path, val, &sjson.Options{ReplaceInPlace: true})
	return
}

// SetAny
func (j *Json) SetAny(path string, val interface{}) (err error) {
	j.data, err = sjson.SetBytesOptions(j.data, path, val, &sjson.Options{ReplaceInPlace: true})
	return
}

// Content
func (j *Json) Content() []byte {
	return j.data
}
