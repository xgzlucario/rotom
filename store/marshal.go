package store

import (
	"fmt"
	"os"
	"time"
)

func (s *Store) marshal() {
	// empty
	if s.m.IsEmpty() {
		return
	}
	// sleep
	if now := time.Now(); now.Sub(s.last) < STORE_DURATION {
		return
	}
	s.marshalForce()
}

func (s *Store) marshalForce() {
	s.last = time.Now()

	// marshal
	buf, _ := s.m.MarshalJSON()
	if err := os.WriteFile(fmt.Sprintf("%s%d.dat", StorePath, s.id), buf, 0666); err != nil {
		panic(err)
	}
}

func (s *Store) unmarshal() {
	buf, err := os.ReadFile(fmt.Sprintf("%s%d.dat", StorePath, s.id))
	if err != nil {
		return
	}
	if err := s.m.UnmarshalJSON(buf); err != nil {
		panic(err)
	}
}
