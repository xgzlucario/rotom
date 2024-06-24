package dict

import (
	"encoding/binary"
	"math/rand/v2"
	"time"

	"github.com/cockroachdb/swiss"
	"github.com/xgzlucario/rotom/internal/pkg"
)

const (
	noTTL = 0
	KB    = 1024

	// maxFailed indicates that the eviction algorithm breaks
	// when consecutive unexpired key-value pairs are detected.
	maxFailed = 3
)

var (
	bufferpool    = pkg.NewBufferPool()
	dictAllocator = pkg.NewAllocator[string, Idx]()
)

// Dict is the hashmap for Rotom.
type Dict struct {
	mask   uint32
	shards []*shard
}

func New(options Options) *Dict {
	dict := &Dict{
		mask:   options.ShardCount - 1,
		shards: make([]*shard, options.ShardCount),
	}
	for i := range dict.shards {
		dict.shards[i] = &shard{
			options: &options,
			index:   swiss.New(options.IndexSize, swiss.WithAllocator(dictAllocator)),
			data:    bufferpool.Get(options.BufferSize)[:0],
		}
	}
	return dict
}

func (dict *Dict) getShard(key string) *shard {
	hash := MemHash(key)
	return dict.shards[uint32(hash)&dict.mask]
}

func (dict *Dict) Get(key string) ([]byte, int64, bool) {
	shard := dict.getShard(key)
	idx, ok := shard.index.Get(key)
	if !ok {
		return nil, 0, false
	}
	if idx.expired() {
		shard.removeEntry(key, idx)
		return nil, 0, false
	}
	_, val := shard.findEntry(idx)
	return val, idx.lo, ok
}

func (dict *Dict) SetTx(key string, val []byte, expiration int64) bool {
	shard := dict.getShard(key)
	idx, ok := shard.index.Get(key)
	if ok {
		entry, oldVal := shard.findEntry(idx)
		// update value inplaced
		if len(val) == len(oldVal) {
			copy(oldVal, val)
			shard.index.Put(key, idx.setTTL(expiration))
			return false
		}
		shard.unused += uint32(len(entry))
	}

	shard.index.Put(key, shard.appendEntry(val, expiration))
	return true
}

func (dict *Dict) Set(kstr string, value []byte) bool {
	return dict.SetTx(kstr, value, noTTL)
}

func (dict *Dict) SetEx(kstr string, value []byte, duration time.Duration) bool {
	return dict.SetTx(kstr, value, time.Now().Add(duration).UnixNano())
}

func (dict *Dict) Remove(key string) bool {
	shard := dict.getShard(key)
	idx, ok := shard.index.Get(key)
	if !ok {
		return false
	}
	shard.removeEntry(key, idx)
	return !idx.expired()
}

func (dict *Dict) SetTTL(key string, expiration int64) bool {
	shard := dict.getShard(key)
	idx, ok := shard.index.Get(key)
	if !ok {
		return false
	}
	if idx.expired() {
		shard.removeEntry(key, idx)
		return false
	}
	shard.index.Put(key, idx.setTTL(expiration))
	return true
}

type Walker func(key string, value []byte, ttl int64) (next bool)

func (dict *Dict) Scan(callback Walker) {
	for _, shard := range dict.shards {
		if !shard.scan(callback) {
			return
		}
	}
}

func (dict *Dict) EvictExpired() {
	id := rand.IntN(len(dict.shards))
	dict.shards[id].evictExpired()
}

// Stats represents the runtime statistics of Dict.
type Stats struct {
	Len      int
	Alloc    uint64
	Unused   uint64
	Migrates uint64
}

// GetStats returns the current runtime statistics of Dict.
func (c *Dict) GetStats() (stats Stats) {
	for _, shard := range c.shards {
		stats.Len += shard.index.Len()
		stats.Alloc += uint64(len(shard.data))
		stats.Unused += uint64(shard.unused)
		stats.Migrates += uint64(shard.migrations)
	}
	return
}

// UnusedRate calculates the percentage of unused space in the dict.
func (s Stats) UnusedRate() float64 {
	return float64(s.Unused) / float64(s.Alloc) * 100
}

// shard is the data container for Dict.
type shard struct {
	options    *Options
	index      *swiss.Map[string, Idx]
	data       []byte
	unused     uint32
	migrations uint32
}

func (s *shard) appendEntry(val []byte, ts int64) Idx {
	idx := newIdx(len(s.data), ts)
	s.data = binary.AppendUvarint(s.data, uint64(len(val)))
	s.data = append(s.data, val...)
	return idx
}

func (s *shard) scan(walker Walker) (next bool) {
	next = true
	s.index.All(func(key string, idx Idx) bool {
		if idx.expired() {
			return true
		}
		_, val := s.findEntry(idx)
		next = walker(key, val, idx.lo)
		return next
	})
	return
}

func (s *shard) evictExpired() {
	var failed int
	nanosec := time.Now().UnixNano()

	// probing
	s.index.All(func(key string, idx Idx) bool {
		failed++
		if idx.expiredWith(nanosec) {
			s.removeEntry(key, idx)
			failed = 0
		}
		return failed <= maxFailed
	})

	// check if migration is needed.
	unusedRate := float64(s.unused) / float64(len(s.data))
	if unusedRate >= s.options.MigrateRatio {
		s.migrate()
	}
}

// migrate transfers valid key-value pairs to a new container to save memory.
func (s *shard) migrate() {
	newData := bufferpool.Get(len(s.data))[:0]
	nanosec := time.Now().UnixNano()
	newIndex := swiss.New(s.index.Len(), swiss.WithAllocator(dictAllocator))

	s.index.All(func(key string, idx Idx) bool {
		if idx.expiredWith(nanosec) {
			return true
		}
		newIndex.Put(key, idx.setStart(len(newData)))
		entry, _ := s.findEntry(idx)
		newData = append(newData, entry...)
		return true
	})

	s.index.Close()
	bufferpool.Put(s.data)
	s.index = newIndex
	s.data = newData
	s.unused = 0
	s.migrations++
}

func (s *shard) findEntry(idx Idx) (entry, val []byte) {
	pos := idx.start()
	// read value len
	vlen, n := binary.Uvarint(s.data[pos:])
	pos += n
	// read value
	val = s.data[pos : pos+int(vlen)]
	pos += int(vlen)

	return s.data[idx.start():pos], val
}

func (s *shard) removeEntry(key string, idx Idx) {
	entry, _ := s.findEntry(idx)
	s.unused += uint32(len(entry))
	s.index.Delete(key)
}
