package structx

import (
	"github.com/zyedidia/generic/list"
)

// Type alias for a block of entries.
type ulistBlk[V any] []V

// UList implements a doubly-linked unolled list.
type UList[V any] struct {
	ll              list.List[ulistBlk[V]]
	entriesPerBlock int
	size            int
}

// New returns an empty unrolled linked list.
// 'entriesPerBlock' is the number of entries to store in each block.
// This value should ideally be the size of a cache-line or multiples there-of.
// See: https://en.wikipedia.org/wiki/Unrolled_linked_list
func NewList[V any](entriesPerBlock int) *UList[V] {
	return &UList[V]{
		ll:              *list.New[ulistBlk[V]](),
		entriesPerBlock: entriesPerBlock,
		size:            0,
	}
}

// Size returns the number of entries in 'ul'.
func (ul *UList[V]) Size() int {
	return ul.size
}

// PushBack adds 'v' to the end of the ulist.
func (ul *UList[V]) PushBack(v V) {
	if !hasCapacity[V](ul.ll.Back) {
		ul.ll.PushBack(ul.newBlock())
	}
	blk := ul.ll.Back.Value
	blk = append(blk, v)
	ul.ll.Back.Value = blk
	ul.size++
}

// PushFront adds 'v' to the beginning of the ulist.
func (ul *UList[V]) PushFront(v V) {
	if !hasCapacity[V](ul.ll.Front) {
		ul.ll.PushFront(ul.newBlock())
	}
	ul.prependToBlock(v, &ul.ll.Front.Value)
	ul.size++
}

// Begin returns an UListIter pointing to the first entry in the UList.
func (ul *UList[V]) Begin() *UListIter[V] {
	return newIterFront(ul)
}

// End returns an UListIter pointing to the last entry in the UList.
func (ul *UList[V]) End() *UListIter[V] {
	return newIterBack(ul)
}

// Remove deletes the entry in 'ul' pointed to by 'iter'.
// 'iter' is moved forward in the process. i.e. iter.Get() returns the element in 'ul'
// that occurs after the deleted entry.
func (ul *UList[V]) Remove(iter *UListIter[V]) {
	ul.size--
	iter.node.Value = append(iter.node.Value[:iter.index], iter.node.Value[iter.index+1:]...)
	if len(iter.node.Value) == 0 {
		// Block got emptied.
		ul.ll.Remove(iter.node)
		iter.Next()
		return
	}
}

func (ul *UList[V]) LPop() (v V, ok bool) {
	s := ul.Begin()
	if s.IsValid() {
		v = s.Get()
		ul.Remove(s)
		return v, true
	}
	return
}

func (ul *UList[V]) RPop() (v V, ok bool) {
	s := ul.End()
	if s.IsValid() {
		v = s.Get()
		ul.Remove(s)
		return v, true
	}
	return
}

func (ul *UList[V]) Index(i int) (v V, ok bool) {
	s := ul.Begin()
	for s.IsValid() {
		if i == 0 {
			return s.Get(), true
		}
		s.Next()
		i--
	}
	return
}

func hasCapacity[V any](llNode *list.Node[ulistBlk[V]]) bool {
	if llNode == nil {
		return false
	}
	return len(llNode.Value) < cap(llNode.Value)
}

func (ul *UList[V]) newBlock() ulistBlk[V] {
	return make([]V, 0, ul.entriesPerBlock)
}

func (ul *UList[V]) prependToBlock(v V, blkPtr *ulistBlk[V]) {
	tmp := ul.newBlock()
	tmp = append(tmp, v)
	// 'append' returns a slice with capacity of the first variable.
	// To maintain the propoer capacity, we use 'tmp' with an explicitly defined capacity.
	*blkPtr = append(tmp, *blkPtr...)
}

// A UListIter points to an element in the UList.
type UListIter[V any] struct {
	node  *list.Node[ulistBlk[V]]
	index int
}

// newIterFront returns a UListIter pointing to the first entry in 'ul'.
// If 'ul' is empty, an invalid iterator is returned.
func newIterFront[V any](ul *UList[V]) *UListIter[V] {
	return &UListIter[V]{
		node:  ul.ll.Front,
		index: 0,
	}
}

// newIterBack returns a UListIter pointing to the last entry in 'ul'.
// If 'ul' is empty, an invalid iterator is returned.
func newIterBack[V any](ul *UList[V]) *UListIter[V] {
	iter := UListIter[V]{
		node:  ul.ll.Back,
		index: 0,
	}
	if iter.node != nil {
		blk := iter.node.Value
		iter.index = len(blk) - 1
	}
	return &iter
}

// IsValid returns true if the iterator points to a valid entry in the UList.
func (iter *UListIter[V]) IsValid() bool {
	if iter.node == nil {
		return false
	}
	blk := iter.node.Value
	return iter.index >= 0 && iter.index < len(blk)
}

// Get returns the entry in the UList that the 'iter' is pointing to.
// This call should only ever be made when iter.IsValid() is true.
func (iter *UListIter[V]) Get() V {
	blk := iter.node.Value
	return blk[iter.index]
}

// Next moves the iterator one step forward and returns true if the iterator is valid.
func (iter *UListIter[V]) Next() bool {
	iter.index++
	blk := iter.node.Value
	if iter.index >= len(blk) {
		if iter.node.Next != nil {
			iter.node = iter.node.Next
			iter.index = 0
		} else {
			// By not going past len, we can recover to the end using Prev().
			iter.index = len(blk)
		}
	}
	return iter.IsValid()
}

// Prev moves the iterator one step back and returns true if the iterator is valid.
func (iter *UListIter[V]) Prev() bool {
	iter.index--
	if iter.index < 0 {
		if iter.node.Prev != nil {
			iter.node = iter.node.Prev
			blk := iter.node.Value
			iter.index = len(blk) - 1
		} else {
			// By not going further past -1, we can recover to the begin using Next().
			iter.index = -1
		}
	}
	return iter.IsValid()
}
