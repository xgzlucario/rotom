package store

import (
	"os"

	"github.com/klauspost/compress/s2"
)

func (s *Store) marshal() {
	if !Persist {
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

	if err := os.WriteFile(s.storePath, src, 0644); err != nil {
		panic(err)
	}
}

func (s *Store) unmarshal() {
	if !Persist {
		return
	}

	src, err := os.ReadFile(s.storePath)
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
