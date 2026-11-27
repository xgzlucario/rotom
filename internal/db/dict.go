package db

import (
	"iter"
	"strings"
	"time"
)

// entry is one key of the keyspace. It is also the node of its hashtable
// bucket chain, and holds the value and expire time inline, so a lookup is a
// single hash probe and a string key costs three objects: the entry, the key
// and the value bytes.
type entry struct {
	key      string
	next     *entry // bucket chain
	expireAt int64  // unix ms, 0 means no expire

	// A value is either a string (obj == nil) or a container. Strings are
	// kept out of `any` so that storing them does not box a slice header.
	str []byte
	obj any
}

func (e *entry) isString() bool { return e.obj == nil }

// value returns the value as stored in the RDB/type switches.
func (e *entry) value() any {
	if e.isString() {
		return e.str
	}
	return e.obj
}

// Dict is the keyspace.
//
// Expired keys are removed lazily on access and actively by ActiveExpire,
// which samples `volatile`, the set of entries with an expire time.
//
// The Dict never retains caller strings: keys are cloned on insert, so
// callers may pass strings aliasing a network buffer.
type Dict struct {
	data     *hashtable
	volatile map[*entry]struct{}

	// now is the cached clock in unix ms. It is refreshed once per command
	// (see Tick) so that a command sees a consistent time, like redis.
	now int64
}

func NewDict() *Dict {
	return &Dict{
		data:     newHashtable(),
		volatile: make(map[*entry]struct{}),
		now:      time.Now().UnixMilli(),
	}
}

// Tick refreshes the cached clock.
func (d *Dict) Tick() { d.now = time.Now().UnixMilli() }

// Now returns the cached clock in unix ms.
func (d *Dict) Now() int64 { return d.now }

// Len returns the number of keys, including expired keys not yet evicted.
func (d *Dict) Len() int { return d.data.Len() }

func (d *Dict) expired(e *entry) bool { return e.expireAt > 0 && e.expireAt <= d.now }

// lookup returns the live entry of key, deleting it if expired.
func (d *Dict) lookup(key string) *entry {
	e := d.data.Get(key)
	if e != nil && d.expired(e) {
		d.Delete(key)
		return nil
	}
	return e
}

// insert adds a new entry for key, which must not exist.
func (d *Dict) insert(key string) *entry {
	e := &entry{key: strings.Clone(key)}
	d.data.Insert(e)
	return e
}

func (d *Dict) setExpire(e *entry, expireAt int64) {
	switch {
	case expireAt > 0 && e.expireAt == 0:
		d.volatile[e] = struct{}{}
	case expireAt == 0 && e.expireAt > 0:
		delete(d.volatile, e)
	}
	e.expireAt = expireAt
}

// Get returns the value of key: []byte for strings, else a container.
func (d *Dict) Get(key string) (any, bool) {
	if e := d.lookup(key); e != nil {
		return e.value(), true
	}
	return nil, false
}

func (d *Dict) Exists(key string) bool { return d.lookup(key) != nil }

// SetString sets key to a copy of val. With keepTTL the expire time is
// kept, otherwise it is replaced by expireAt (unix ms, 0 = none).
func (d *Dict) SetString(key string, val []byte, expireAt int64, keepTTL bool) {
	e := d.lookup(key)
	if e == nil {
		e = d.insert(key)
	}
	// reuse the old buffer when it fits: overwrites do not allocate
	if e.isString() && cap(e.str) >= len(val) && cap(e.str) <= 2*len(val)+16 {
		e.str = append(e.str[:0], val...)
	} else {
		e.str = append(make([]byte, 0, len(val)), val...)
	}
	e.obj = nil
	if !keepTTL {
		d.setExpire(e, expireAt)
	}
}

// setObject sets key to a container, keeping the expire time.
func (d *Dict) setObject(key string, obj any) {
	e := d.lookup(key)
	if e == nil {
		e = d.insert(key)
	}
	e.str, e.obj = nil, obj
}

// Delete removes key and reports whether a live key was removed.
func (d *Dict) Delete(key string) bool {
	e := d.data.Delete(key)
	if e == nil {
		return false
	}
	delete(d.volatile, e)
	return !d.expired(e)
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
		d.Delete(key)
	} else {
		d.setExpire(e, expireAt)
	}
	return true
}

// Persist removes the expire time of key. It reports whether a timeout was removed.
func (d *Dict) Persist(key string) bool {
	e := d.lookup(key)
	if e == nil || e.expireAt == 0 {
		return false
	}
	d.setExpire(e, 0)
	return true
}

// Item is a key as seen by Dict.All.
type Item struct {
	Key      string
	Value    any
	ExpireAt int64
}

// All iterates live keys. The dict must not be used meanwhile: even reads
// advance incremental rehashing.
func (d *Dict) All() iter.Seq[Item] {
	return func(yield func(Item) bool) {
		for e := range d.live() {
			if !yield(Item{e.key, e.value(), e.expireAt}) {
				return
			}
		}
	}
}

func (d *Dict) live() iter.Seq[*entry] {
	return func(yield func(*entry) bool) {
		for e := range d.data.All() {
			if !d.expired(e) && !yield(e) {
				return
			}
		}
	}
}

// Scan visits keys from cursor on and returns the next cursor (0 when done),
// following redis SCAN guarantees. Expired keys are skipped.
func (d *Dict) Scan(cursor uint64, fn func(key string, value any)) uint64 {
	return d.data.Scan(cursor, func(e *entry) {
		if !d.expired(e) {
			fn(e.key, e.value())
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
		// map iteration starts at a random position: a cheap random sample.
		for e := range d.volatile {
			if sampled == activeExpireSamples {
				break
			}
			sampled++
			if d.expired(e) {
				d.Delete(e.key)
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
