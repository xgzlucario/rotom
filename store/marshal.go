package store

import (
	"fmt"
	"os"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/xgzlucario/rotom/base"
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

type dbJSON struct {
	B []byte
}

func (s *Store) marshalForce() {
	s.last = time.Now()

	// marshal
	src, _ := s.m.MarshalJSON()

	// Compress
	src = s2.EncodeSnappy(nil, src)

	// marshal again
	src, _ = base.MarshalJSON(dbJSON{src})

	if err := os.WriteFile(fmt.Sprintf("%s%d.dat", StorePath, s.id), src, 0666); err != nil {
		panic(err)
	}
}

func (s *Store) unmarshal() {
	src, err := os.ReadFile(fmt.Sprintf("%s%d.dat", StorePath, s.id))
	if err != nil {
		return
	}

	// unmarshal
	var tmp dbJSON
	if err := base.UnmarshalJSON(src, &tmp); err != nil {
		panic(err)
	}

	// Decompress
	tmp.B, err = s2.Decode(nil, tmp.B)
	if err != nil {
		panic(err)
	}

	// unmarshal
	if err := s.m.UnmarshalJSON(tmp.B); err != nil {
		panic(err)
	}
}
