package store

import "time"

// Session
type Session struct {
	store
	sdmap map[*storeShard]struct{}
}

// NewSession
func NewSession() *Session {
	return &Session{
		store: db,
		sdmap: make(map[*storeShard]struct{}),
	}
}

// Set
func (s *Session) Set(key string, val any) {
	s.sdmap[s.getShard(key)] = struct{}{}

	s.store.Set(key, val)
}

// SetWithTTL
func (s *Session) SetWithTTL(key string, val any, ttl time.Duration) {
	s.sdmap[s.getShard(key)] = struct{}{}

	s.store.SetWithTTL(key, val, ttl)
}

// Remove
func (s *Session) Remove(key string) (any, bool) {
	s.sdmap[s.getShard(key)] = struct{}{}

	return s.store.Remove(key)
}

// Persist
func (s *Session) Persist(key string) bool {
	s.sdmap[s.getShard(key)] = struct{}{}

	return s.store.Persist(key)
}

// Commit
func (s *Session) Commit() error {
	for sd := range s.sdmap {
		if _, err := sd.flushBuffer(); err != nil {
			return err
		}
	}
	s.sdmap = make(map[*storeShard]struct{})
	return nil
}
