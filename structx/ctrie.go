package structx

import "sync"

const SHARD_COUNT = 32

type CTrieShard[V any] struct {
	tree *Trie[V]
	sync.RWMutex
}

type CTrie[V any] struct {
	shards []*CTrieShard[V]
}

// NewCTrie
func NewCTrie[V any]() *CTrie[V] {
	ct := &CTrie[V]{
		shards: make([]*CTrieShard[V], 32),
	}
	for i := range ct.shards {
		ct.shards[i] = &CTrieShard[V]{tree: NewTrie[V]()}
	}
	return ct
}

// Put
func (ct *CTrie[V]) Put(key string, val V) {
	shard := ct.getShard(key)
	shard.Lock()
	shard.tree.Put(key, val)
	shard.Unlock()
}

// Put
func (ct *CTrie[V]) Get(key string) (V, bool) {
	shard := ct.getShard(key)
	shard.RLock()
	val, ok := shard.tree.Get(key)
	shard.RUnlock()
	return val, ok
}

// Remove
func (ct *CTrie[V]) Remove(key string) {
	shard := ct.getShard(key)
	shard.Lock()
	shard.tree.Remove(key)
	shard.Unlock()
}

// Count
func (ct *CTrie[V]) Count() int {
	var sum int
	for _, shard := range ct.shards {
		sum += shard.tree.Size()
	}
	return sum
}

// Keys
func (ct *CTrie[V]) Keys() []string {
	keys := make([]string, 0, ct.Count())
	for _, shard := range ct.shards {
		keys = append(keys, shard.tree.Keys()...)
	}
	return keys
}

// getShard
func (ct *CTrie[V]) getShard(key string) *CTrieShard[V] {
	return ct.shards[fnv32(key)%SHARD_COUNT]
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	keyLength := len(key)
	for i := 0; i < keyLength; i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}
