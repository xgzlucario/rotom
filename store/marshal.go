package store

import (
	"fmt"
	"os"

	"github.com/klauspost/compress/s2"
	"github.com/xgzlucario/rotom/base"
)

type dbJSON struct {
	B []byte
}

func (s *Store) marshal() {
	if !s.Persist {
		return
	}
	
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
	if !s.Persist {
		return
	}

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
