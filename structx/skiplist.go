package structx

import (
	"fmt"
	"math/rand"

	"github.com/xgzlucario/rotom/base"
)

const maxLevel = 32
const pFactor = 0.25

type skiplistNode[K, V base.Ordered] struct {
	key   K
	value V
	next  []*skiplistNode[K, V]
}

type Skiplist[K, V base.Ordered] struct {
	level int
	len   int
	head  *skiplistNode[K, V]
}

// NewSkipList
func NewSkipList[K, V base.Ordered]() *Skiplist[K, V] {
	return &Skiplist[K, V]{
		head: &skiplistNode[K, V]{
			next: make([]*skiplistNode[K, V], maxLevel),
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

// GetByRank: Get the element by rank
func (s *Skiplist[K, V]) GetByRank(rank int) (k K, v V, err error) {
	p := s.head
	for i := 0; p != nil; i++ {
		if rank == i {
			return p.key, p.value, nil
		}
		p = p.next[0]
	}
	return k, v, base.ErrOutOfBounds(rank)
}

// GetScoreWithRank: Get the score and rank by key
func (s *Skiplist[K, V]) GetScoreWithRank(key K) (v V, rank int, err error) {
	p := s.head
	for i := 0; p != nil; i++ {
		if p.key == key {
			return p.value, i, nil
		}
		p = p.next[0]
	}
	return v, -1, base.ErrOutOfBounds(rank)
}

func (s *Skiplist[K, V]) findClosestNode(key K, value V, update []*skiplistNode[K, V]) *skiplistNode[K, V] {
	p := s.head
	for i := s.level - 1; i >= 0; i-- {
		// Find the elem at level[i] that closest to value and key
		for p.next[i] != nil && (p.next[i].value < value || (p.next[i].value == value && p.next[i].key < key)) {
			p = p.next[i]
		}
		update[i] = p
	}
	return p
}

// Find
func (s *Skiplist[K, V]) Find(value V) bool {
	p := s.head
	for i := s.level - 1; i >= 0; i-- {
		for p.next[i] != nil && p.next[i].value < value {
			p = p.next[i]
		}
	}
	p = p.next[0]
	return p != nil && p.value == value
}

// Add
func (s *Skiplist[K, V]) Add(key K, value V) *skiplistNode[K, V] {
	update := make([]*skiplistNode[K, V], maxLevel)
	for i := range update {
		update[i] = s.head
	}

	s.findClosestNode(key, value, update)

	lv := randomLevel()
	if lv > s.level {
		s.level = lv
	}

	// create node
	newNode := &skiplistNode[K, V]{
		key: key, value: value, next: make([]*skiplistNode[K, V], lv),
	}

	for i, node := range update[:lv] {
		// Update the state at level[i], pointing the next of the current element to the new node
		newNode.next[i] = node.next[i]
		node.next[i] = newNode
	}

	s.len++
	return newNode
}

// Delete
func (s *Skiplist[K, V]) Delete(key K, value V) bool {
	update := make([]*skiplistNode[K, V], maxLevel)

	p := s.findClosestNode(key, value, update)

	p = p.next[0]
	if p == nil || p.value != value {
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

// Range
func (s *Skiplist[K, V]) Range(start, end int, f func(K, V) bool) {
	if end == -1 {
		end = s.Len()
	}
	p := s.head.next[0]
	for i := 0; p != nil; i++ {
		// index
		if start <= i && i <= end {
			if f(p.key, p.value) {
				return
			}
		}
		p = p.next[0]
	}
}

// RangeByScores
func (s *Skiplist[K, V]) RangeByScores(min, max V, f func(K, V) bool) {
	p := s.head.next[0]
	for p != nil {
		if min <= p.value && p.value <= max {
			if f(p.key, p.value) {
				return
			}
		}
		p = p.next[0]
	}
}

// Print
func (s *Skiplist[K, V]) Print() {
	s.Range(0, -1, func(key K, value V) bool {
		fmt.Printf("%+v -> %+v\n", key, value)
		return false
	})
}
