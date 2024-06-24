package hash

import (
	"github.com/cockroachdb/swiss"
	"github.com/xgzlucario/rotom/internal/pkg"
)

var (
	setAllocator = pkg.NewAllocator[string, struct{}]()
)

type Set struct {
	m *swiss.Map[string, struct{}]
}

func NewSet() *Set {
	return &Set{m: swiss.New[string, struct{}](8, swiss.WithAllocator(setAllocator))}
}

func (s *Set) Add(key string) bool {
	if _, ok := s.m.Get(key); ok {
		return false
	}
	s.m.Put(key, struct{}{})
	return true
}

func (s *Set) Pop() (item string, ok bool) {
	s.m.All(func(key string, _ struct{}) bool {
		s.m.Delete(key)
		item, ok = key, true
		return false
	})
	return
}

func (s *Set) Free() {
	s.m.Close()
}
