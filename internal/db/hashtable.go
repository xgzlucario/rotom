package db

import (
	"hash/maphash"
	"iter"
	"math/bits"
)

// hashtable is a port of the redis dict: chained buckets with power-of-two
// sizes, incremental rehashing, and a stateless reverse-binary scan cursor.
//
// Go maps cannot provide a resumable SCAN cursor, which is why the keyspace
// uses this table. It is intrusive: entries are the chain nodes themselves
// (entry.next), so a key costs a single allocation.
type hashtable struct {
	ht        [2]table
	rehashIdx int // -1 when not rehashing
	seed      maphash.Seed
}

type table struct {
	buckets []*entry
	used    int
}

const (
	htInitSize = 4
	// shrink when fewer than 1/htMinFillInv of the buckets are used.
	htMinFillInv = 8
)

func newHashtable() *hashtable {
	return &hashtable{rehashIdx: -1, seed: maphash.MakeSeed()}
}

func (t *table) mask() uint64 { return uint64(len(t.buckets) - 1) }

func (h *hashtable) hash(key string) uint64 { return maphash.String(h.seed, key) }

func (h *hashtable) Len() int { return h.ht[0].used + h.ht[1].used }

func (h *hashtable) rehashing() bool { return h.rehashIdx >= 0 }

// rehashStep moves up to n buckets from ht[0] to ht[1], visiting at most
// 10*n empty buckets, so that each step has bounded cost.
func (h *hashtable) rehashStep(n int) {
	if !h.rehashing() {
		return
	}
	emptyVisits := n * 10
	t0, t1 := &h.ht[0], &h.ht[1]
	for ; n > 0 && t0.used > 0; n-- {
		for t0.buckets[h.rehashIdx] == nil {
			h.rehashIdx++
			if emptyVisits--; emptyVisits == 0 {
				return
			}
		}
		for x := t0.buckets[h.rehashIdx]; x != nil; {
			next := x.next
			idx := h.hash(x.key) & t1.mask()
			x.next = t1.buckets[idx]
			t1.buckets[idx] = x
			t0.used--
			t1.used++
			x = next
		}
		t0.buckets[h.rehashIdx] = nil
		h.rehashIdx++
	}
	if t0.used == 0 {
		h.ht[0] = h.ht[1]
		h.ht[1] = table{}
		h.rehashIdx = -1
	}
}

// resize starts rehashing into a table of at least size buckets.
func (h *hashtable) resize(size int) {
	n := htInitSize
	for n < size {
		n *= 2
	}
	if n == len(h.ht[0].buckets) {
		return
	}
	if h.ht[0].buckets == nil {
		h.ht[0].buckets = make([]*entry, n)
		return
	}
	h.ht[1] = table{buckets: make([]*entry, n)}
	h.rehashIdx = 0
}

func (h *hashtable) expandIfNeeded() {
	if h.rehashing() {
		return
	}
	if h.ht[0].buckets == nil {
		h.resize(htInitSize)
	} else if h.ht[0].used >= len(h.ht[0].buckets) {
		h.resize(h.ht[0].used * 2)
	}
}

func (h *hashtable) shrinkIfNeeded() {
	if h.rehashing() {
		return
	}
	size := len(h.ht[0].buckets)
	if size > htInitSize && h.ht[0].used*htMinFillInv < size {
		h.resize(h.ht[0].used)
	}
}

// Get returns the entry of key, or nil.
func (h *hashtable) Get(key string) *entry {
	if h.Len() == 0 {
		return nil
	}
	h.rehashStep(1)
	hv := h.hash(key)
	for i := range 2 {
		t := &h.ht[i]
		if t.buckets != nil {
			for x := t.buckets[hv&t.mask()]; x != nil; x = x.next {
				if x.key == key {
					return x
				}
			}
		}
		if !h.rehashing() {
			break
		}
	}
	return nil
}

// Insert adds e, whose key must not be present.
func (h *hashtable) Insert(e *entry) {
	h.rehashStep(1)
	h.expandIfNeeded()
	t := &h.ht[0]
	if h.rehashing() {
		t = &h.ht[1] // new keys go to the new table
	}
	idx := h.hash(e.key) & t.mask()
	e.next = t.buckets[idx]
	t.buckets[idx] = e
	t.used++
}

// Delete removes key and returns its entry, or nil.
func (h *hashtable) Delete(key string) *entry {
	if h.Len() == 0 {
		return nil
	}
	h.rehashStep(1)
	hv := h.hash(key)
	for i := range 2 {
		t := &h.ht[i]
		if t.buckets == nil {
			continue
		}
		idx := hv & t.mask()
		var prev *entry
		for x := t.buckets[idx]; x != nil; prev, x = x, x.next {
			if x.key != key {
				continue
			}
			if prev == nil {
				t.buckets[idx] = x.next
			} else {
				prev.next = x.next
			}
			x.next = nil
			t.used--
			h.shrinkIfNeeded()
			return x
		}
		if !h.rehashing() {
			break
		}
	}
	return nil
}

// All iterates every entry. The table must not be modified meanwhile.
func (h *hashtable) All() iter.Seq[*entry] {
	return func(yield func(*entry) bool) {
		for i := range h.ht {
			for _, x := range h.ht[i].buckets {
				for ; x != nil; x = x.next {
					if !yield(x) {
						return
					}
				}
			}
		}
	}
}

// Scan visits the bucket(s) addressed by cursor and returns the next
// cursor; 0 means the iteration is complete.
//
// The cursor is incremented in reverse-bit order, so a resize between calls
// never makes it skip a bucket: every key present during the whole scan is
// returned at least once (some may be returned more than once).
func (h *hashtable) Scan(cursor uint64, fn func(*entry)) uint64 {
	if h.Len() == 0 {
		return 0
	}
	emit := func(t *table, idx uint64) {
		for x := t.buckets[idx]; x != nil; x = x.next {
			fn(x)
		}
	}
	v := cursor
	if !h.rehashing() {
		t0 := &h.ht[0]
		m0 := t0.mask()
		emit(t0, v&m0)
		v |= ^m0
		return bits.Reverse64(bits.Reverse64(v) + 1)
	}

	// While rehashing, visit the bucket in the small table and every bucket
	// of the large table it expands to.
	t0, t1 := &h.ht[0], &h.ht[1]
	if len(t0.buckets) > len(t1.buckets) {
		t0, t1 = t1, t0
	}
	m0, m1 := t0.mask(), t1.mask()
	emit(t0, v&m0)
	for {
		emit(t1, v&m1)
		v |= ^m1
		v = bits.Reverse64(bits.Reverse64(v) + 1)
		if v&(m0^m1) == 0 {
			break
		}
	}
	return v
}
