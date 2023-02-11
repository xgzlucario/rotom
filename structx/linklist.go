package structx

import "fmt"

/*
Create by ChatGPT
Complete the LinkList using Go:
1. Elements in this list are sortable.
2. Includes Constructor, Insert() *linkItem[T], Remove(*linkItem[T]), RemoveFirst(T), Print().
4. The default value of nil or 0 does not need to be specified.
*/
type LinkList[T comparable] struct {
	head *linkItem[T]
	tail *linkItem[T]
	less func(T, T) bool // if t1 < t2 return true
}

type linkItem[T comparable] struct {
	val  T
	prev *linkItem[T]
	next *linkItem[T]
}

func NewLinkList[T comparable](less func(T, T) bool) *LinkList[T] {
	return &LinkList[T]{
		less: less,
	}
}

func (l *LinkList[T]) Insert(val T) *linkItem[T] {
	if l.head == nil {
		l.head = &linkItem[T]{
			val: val,
		}
		l.tail = l.head
		return l.head
	}
	if l.less(val, l.head.val) {
		l.head.prev = &linkItem[T]{
			val:  val,
			next: l.head,
		}
		l.head = l.head.prev
		return l.head
	}
	if l.less(l.tail.val, val) {
		l.tail.next = &linkItem[T]{
			val:  val,
			prev: l.tail,
		}
		l.tail = l.tail.next
		return l.tail
	}
	for cur := l.head; cur != nil; cur = cur.next {
		if l.less(cur.val, val) && l.less(val, cur.next.val) {
			cur.next.prev = &linkItem[T]{
				val:  val,
				prev: cur,
				next: cur.next,
			}
			cur.next = cur.next.prev
			return cur.next
		}
	}
	return nil
}

func (l *LinkList[T]) Remove(item *linkItem[T]) {
	if item == nil {
		return
	}
	if item.prev == nil {
		l.head = item.next
	} else {
		item.prev.next = item.next
	}
	if item.next == nil {
		l.tail = item.prev
	} else {
		item.next.prev = item.prev
	}
}

func (l *LinkList[T]) RemoveFirst(val T) {
	for cur := l.head; cur != nil; cur = cur.next {
		if cur.val == val {
			l.Remove(cur)
			return
		}
	}
}

func (l *LinkList[T]) Print() {
	for cur := l.head; cur != nil; cur = cur.next {
		fmt.Printf("%v ", cur.val)
	}
	fmt.Println()
}
