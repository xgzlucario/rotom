package structx

import (
	"math/rand"

	"github.com/xgzlucario/rotom/base"
)

const maxLevel = 32
const pFactor = 0.25

type sklNode[K base.Ordered, V any] struct {
	key   K
	value V
	next  []*sklNode[K, V]
}

type Skiplist[K base.Ordered, V any] struct {
	level int
	len   int
	head  *sklNode[K, V]
}

// NewSkipList
func NewSkipList[K base.Ordered, V any]() *Skiplist[K, V] {
	return &Skiplist[K, V]{
		head: &sklNode[K, V]{
			next: make([]*sklNode[K, V], maxLevel),
		},
	}
}

func randomLevel() int {
	lv := 1
	for float32(rand.Int31()&0xFFFF) < (pFactor * 0xFFFF) {
		lv++
	}
	if lv < maxLevel {
		return lv
	}
	return maxLevel
}

func (s *Skiplist[K, V]) Len() int {
	return s.len
}

// find the closest node by key
func (s *Skiplist[K, V]) findClosestNode(key K, update []*sklNode[K, V]) *sklNode[K, V] {
	p := s.head
	for i := s.level - 1; i >= 0; i-- {
		// Find the elem at level[i] that closest to value and key
		for p.next[i] != nil && p.next[i].key < key {
			p = p.next[i]
		}
		update[i] = p
	}
	return p
}

// Insert key not exist or update value when key exist
func (s *Skiplist[K, V]) Insert(key K, value V) *sklNode[K, V] {
	update := make([]*sklNode[K, V], maxLevel)
	for i := range update {
		update[i] = s.head
	}

	p := s.findClosestNode(key, update).next[0]
	// key exist
	if p != nil && p.key == key {
		p.value = value
		return p
	}

	lv := randomLevel()
	if lv > s.level {
		s.level = lv
	}

	// create node
	newNode := &sklNode[K, V]{
		key: key, value: value, next: make([]*sklNode[K, V], lv),
	}

	for i, node := range update[:lv] {
		// Update the state at level[i], pointing the next of the current element to the new node
		newNode.next[i] = node.next[i]
		node.next[i] = newNode
	}

	s.len++
	return newNode
}

// Delete return true when key exist
func (s *Skiplist[K, V]) Delete(key K) bool {
	update := make([]*sklNode[K, V], maxLevel)

	p := s.findClosestNode(key, update).next[0]
	if p == nil || p.key != key {
		return false
	}

	for i := 0; i < s.level && update[i].next[i] == p; i++ {
		// Update the state of levek[i] to point next to the next hop of the deleted node
		update[i].next[i] = p.next[i]
	}

	// Update current level
	for s.level > 1 && s.head.next[s.level-1] == nil {
		s.level--
	}

	s.len--
	return true
}

// Iter return an iterator
func (s *Skiplist[K, V]) Iter() *sklNode[K, V] {
	return s.head.next[0]
}

// Next return next node
func (s *sklNode[K, V]) Next() *sklNode[K, V] {
	return s.next[0]
}

// Key return key of node
func (s *sklNode[K, V]) Key() K {
	return s.key
}

// Value return value of node
func (s *sklNode[K, V]) Value() V {
	return s.value
}

// MarshalJSON
func (s *Skiplist[K, V]) MarshalJSON() ([]byte, error) {
	tmp := base.GTreeJSON[K, V]{
		K: make([]K, 0, s.len),
		V: make([]V, 0, s.len),
	}
	for it := s.Iter(); it != nil; it = it.Next() {
		tmp.K = append(tmp.K, it.key)
		tmp.V = append(tmp.V, it.value)
	}

	return base.MarshalJSON(tmp)
}

// UnmarshalJSON
func (s *Skiplist[K, V]) UnmarshalJSON(src []byte) error {
	var tmp base.GTreeJSON[K, V]
	if err := base.UnmarshalJSON(src, &tmp); err != nil {
		return err
	}
	for i, k := range tmp.K {
		s.Insert(k, tmp.V[i])
	}

	return nil
}
