package hash

import (
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/xgzlucario/rotom/internal/iface"
	"github.com/xgzlucario/rotom/internal/resp"
)

const (
	defaultSetSize = 512
)

type SetI interface {
	iface.Encoder
	Add(key string) bool
	Exist(key string) bool
	Remove(key string) bool
	Pop() (key string, ok bool)
	Scan(fn func(key string))
	Len() int
}

var _ SetI = (*Set)(nil)

type Set struct {
	mapset.Set[string]
}

func NewSet() *Set {
	return &Set{mapset.NewThreadUnsafeSetWithSize[string](defaultSetSize)}
}

func (s Set) Remove(key string) bool {
	if !s.Exist(key) {
		return false
	}
	s.Set.Remove(key)
	return true
}

func (s Set) Scan(fn func(string)) {
	s.Set.Each(func(s string) bool {
		fn(s)
		return false
	})
}

func (s Set) Exist(key string) bool { return s.Set.ContainsOne(key) }

func (s Set) Len() int { return s.Cardinality() }

func (s Set) Encode(writer *resp.Writer) error {
	writer.WriteArrayHead(s.Len())
	s.Scan(func(key string) {
		writer.WriteBulkString(key)
	})
	return nil
}

func (s Set) Decode(reader *resp.Reader) error {
	n, err := reader.ReadArrayHead()
	if err != nil {
		return err
	}
	for range n {
		key, err := reader.ReadBulk()
		if err != nil {
			return err
		}
		s.Add(string(key))
	}
	return nil
}
