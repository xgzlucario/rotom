package structx

import (
	"bytes"
	"slices"

	"github.com/xgzlucario/quicklist"
)

type Zipmap struct {
	data *quicklist.ListPack
}

func NewZipmap() *Zipmap {
	return &Zipmap{quicklist.NewListPack()}
}

func (m *Zipmap) GetType() Type {
	return TypeZipmap
}

func (m *Zipmap) Get(key string) (val []byte, ok bool) {
	var next bool
	m.data.Range(0, -1, func(data []byte, index int) (stop bool) {
		if next {
			ok = true
			val = slices.Clone(data)
			return true
		}
		if bytes.Equal(data, s2b(&key)) {
			next = true
		}
		return false
	})
	return
}

func (m *Zipmap) Set(key string, value []byte) {
	m.data.Insert(-1, key, b2s(value))
}

func (m *Zipmap) Remove(key string) (ok bool) {
	index, ok := m.data.RemoveFirst(key)
	if ok {
		m.data.Remove(index)
	}
	return ok
}

func (m *Zipmap) Len() int {
	return m.data.Size() / 2
}

func (m *Zipmap) Keys() []string {
	keys := make([]string, 0, m.Len())
	m.data.Range(0, -1, func(data []byte, index int) (stop bool) {
		if index%2 == 0 {
			keys = append(keys, string(data))
		}
		return false
	})
	return keys
}

func (m *Zipmap) MarshalJSON() ([]byte, error) {
	return m.data.ToBytes(), nil
}

func (m *Zipmap) UnmarshalJSON(data []byte) error {
	lp, err := quicklist.NewFromBytes(data)
	if err != nil {
		return err
	}
	m.data = lp
	return nil
}
