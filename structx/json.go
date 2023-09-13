package structx

import (
	"sync"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type Json struct {
	sync.RWMutex
	data []byte
}

// NewJson
func NewJson() *Json {
	return &Json{data: make([]byte, 0)}
}

// Get
func (j *Json) Get(path string) any {
	j.Lock()
	defer j.Unlock()

	return gjson.GetBytes(j.data, path).Value()
}

// GetBytes
func (j *Json) GetBytes(path string) []any {
	j.Lock()
	defer j.Unlock()

	res := gjson.GetManyBytes(j.data, path)
	arr := make([]any, 0, len(res))
	for _, v := range res {
		arr = append(arr, v.Value())
	}

	return arr
}

// SetBytes
func (j *Json) SetBytes(path string, val []byte) (err error) {
	j.Lock()
	j.data, err = sjson.SetBytesOptions(j.data, path, val, &sjson.Options{ReplaceInPlace: true})
	j.Unlock()

	return
}

// SetAny
func (j *Json) SetAny(path string, val interface{}) (err error) {
	j.Lock()
	j.data, err = sjson.SetBytesOptions(j.data, path, val, &sjson.Options{ReplaceInPlace: true})
	j.Unlock()

	return
}

// Delete
func (j *Json) Delete(path string) (err error) {
	j.Lock()
	j.data, err = sjson.DeleteBytes(j.data, path)
	j.Unlock()

	return
}

// Content
func (j *Json) Content() []byte {
	return j.data
}
