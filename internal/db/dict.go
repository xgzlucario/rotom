package db

import (
	"strings"
	"time"
)

// entry is a value with its expire time stored inline, so that a lookup
// needs a single hash probe.
//
// Entries are stored by pointer and updated in place. This matters: for
// string keys, assigning to an existing map key also replaces the stored key,
// and callers usually pass keys that alias the network buffer.
type entry struct {
	value    any
	expireAt int64 // unix ms, 0 means no expire
}

// Dict is the keyspace of rotom.
//
// Keys live in a redis-style hashtable (see hashtable.go), which supports
// SCAN cursors. Expired keys are removed lazily on access and actively by
// ActiveExpire, which samples the `volatile` index (keys with an expire time).
type Dict struct {
	data     *hashtable
	volatile map[string]struct{}

	// now is the cached clock in unix ms. It is refreshed once per command
	// (see Tick) so that a command sees a consistent time, like redis.
	now int64
}

func NewDict() *Dict {
	return &Dict{
		data:     newHashtable(),
		volatile: make(map[string]struct{}, 64),
		now:      time.Now().UnixMilli(),
	}
}

// Tick refreshes the cached clock.
func (d *Dict) Tick() { d.now = time.Now().UnixMilli() }

// Now returns the cached clock in unix ms.
func (d *Dict) Now() int64 { return d.now }

func (d *Dict) Len() int { return d.data.Len() }

// lookup returns the live entry of key, deleting it if expired.
func (d *Dict) lookup(key string) *entry {
	e := d.data.Get(key)
	if e == nil {
		return nil
	}
	if e.expireAt > 0 && e.expireAt <= d.now {
		d.delete(key)
		return nil
	}
	return e
}

// Get returns the value of key.
func (d *Dict) Get(key string) (any, bool) {
	if e := d.lookup(key); e != nil {
		return e.value, true
	}
	return nil, false
}

// Set sets the value of key and keeps its expire time, if any.
//
// The Dict never retains key: it is cloned on insert, so callers may pass
// strings that alias a network buffer.
func (d *Dict) Set(key string, value any) {
	if e := d.lookup(key); e != nil {
		e.value = value
		return
	}
	d.data.Insert(strings.Clone(key), &entry{value: value})
}

// SetWithExpire sets the value of key and replaces its expire time.
// expireAt is unix ms; 0 removes the expire time. Key ownership as in Set.
func (d *Dict) SetWithExpire(key string, value any, expireAt int64) {
	e := d.lookup(key)
	if e == nil {
		e = &entry{}
		d.data.Insert(strings.Clone(key), e)
	}
	e.value = value
	d.setExpire(key, e, expireAt)
}

func (d *Dict) setExpire(key string, e *entry, expireAt int64) {
	switch {
	case expireAt > 0 && e.expireAt == 0:
		// clone: key may alias a network buffer
		d.volatile[strings.Clone(key)] = struct{}{}
	case expireAt == 0 && e.expireAt > 0:
		delete(d.volatile, key)
	}
	e.expireAt = expireAt
}

func (d *Dict) delete(key string) {
	d.data.Delete(key)
	delete(d.volatile, key)
}

// Delete removes key and reports whether a live key was removed.
func (d *Dict) Delete(key string) bool {
	if d.lookup(key) == nil {
		return false
	}
	d.delete(key)
	return true
}

// ExpireAt returns the expire time of key (0 if it has none) and whether it exists.
func (d *Dict) ExpireAt(key string) (int64, bool) {
	if e := d.lookup(key); e != nil {
		return e.expireAt, true
	}
	return 0, false
}

// SetExpireAt sets the expire time (unix ms) of an existing key.
// A time in the past deletes the key. It reports whether the key exists.
func (d *Dict) SetExpireAt(key string, expireAt int64) bool {
	e := d.lookup(key)
	if e == nil {
		return false
	}
	if expireAt <= d.now {
		d.delete(key)
		return true
	}
	d.setExpire(key, e, expireAt)
	return true
}

// Persist removes the expire time of key. It reports whether a timeout was removed.
func (d *Dict) Persist(key string) bool {
	e := d.lookup(key)
	if e == nil || e.expireAt == 0 {
		return false
	}
	d.setExpire(key, e, 0)
	return true
}

// Range calls fn for every live key until fn returns false.
// fn must not modify the dict.
func (d *Dict) Range(fn func(key string, value any, expireAt int64) bool) {
	d.data.All(func(key string, e *entry) bool {
		if e.expireAt > 0 && e.expireAt <= d.now {
			return true
		}
		return fn(key, e.value, e.expireAt)
	})
}

// Scan visits keys from cursor on and returns the next cursor (0 when done),
// following redis SCAN guarantees. Expired keys are skipped.
func (d *Dict) Scan(cursor uint64, fn func(key string, value any)) uint64 {
	return d.data.Scan(cursor, func(key string, e *entry) {
		if e.expireAt == 0 || e.expireAt > d.now {
			fn(key, e.value)
		}
	})
}

const (
	activeExpireSamples    = 20
	activeExpireAcceptable = activeExpireSamples / 4 // keep going while >25% expired
)

// ActiveExpire implements redis' active expire cycle: sample keys with a TTL,
// delete the expired ones, and repeat while more than 25% of a sample was
// expired and the time budget allows. It returns the number of deleted keys.
func (d *Dict) ActiveExpire(budget time.Duration) int {
	d.Tick()
	deadline := time.Now().Add(budget)
	total := 0
	for len(d.volatile) > 0 {
		sampled, expired := 0, 0
		// map iteration starts at a random position, which makes this a
		// cheap random sample.
		for key := range d.volatile {
			if sampled == activeExpireSamples {
				break
			}
			sampled++
			if e := d.data.Get(key); e == nil || e.expireAt <= d.now {
				d.delete(key)
				expired++
			}
		}
		total += expired
		if expired <= activeExpireAcceptable || time.Now().After(deadline) {
			break
		}
	}
	return total
}
