package store

import (
	"fmt"
	"os"

	"github.com/klauspost/compress/s2"
)

func (s *Store) marshal() {
	if !s.persist {
		return
	}
	// empty
	if s.m.IsEmpty() {
		return
	}

	// marshal
	src, _ := s.m.MarshalJSON()

	// Compress
	src = s2.EncodeSnappy(nil, src)

	if err := os.WriteFile(fmt.Sprintf("%s%d.bin", StorePath, s.id), src, 0666); err != nil {
		panic(err)
	}
}

func (s *Store) unmarshal() {
	if !s.persist {
		return
	}

	src, err := os.ReadFile(fmt.Sprintf("%s%d.bin", StorePath, s.id))
	if err != nil {
		return
	}

	// Decompress
	src, err = s2.Decode(nil, src)
	if err != nil {
		panic(err)
	}

	// unmarshal
	if err := s.m.UnmarshalJSON(src); err != nil {
		panic(err)
	}
}
