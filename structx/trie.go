package structx

import (
	"github.com/xgzlucario/rotom/base"
)

// Trie
type Trie[V any] struct {
	n    int
	root *node[V]
}

type node[V any] struct {
	c                byte
	valid            bool
	val              V
	left, mid, right *node[V]
}

func (n *node[V]) isUnused() bool {
	return !n.valid && n.mid == nil
}

func (n *node[V]) delete() *node[V] {
	if n == nil {
		return nil
	}
	if n.right == nil {
		return n.left
	}
	if n.left == nil {
		return n.right
	}

	deleted := n
	n = deleted.right.minChild()
	n.right = deleted.right.deleteMinChild()
	n.left = deleted.left
	return n
}

func (n *node[V]) minChild() *node[V] {
	if n == nil {
		return nil
	}

	for n.left != nil {
		n = n.left
	}
	return n
}

func (n *node[V]) deleteMinChild() *node[V] {
	if n == nil {
		return nil
	}

	if n.left == nil { // n is the min node
		return n.right
	}

	n.left = n.left.deleteMinChild()
	return n
}

// NewTrie
func NewTrie[V any]() *Trie[V] {
	return &Trie[V]{}
}

// Size
func (t *Trie[V]) Size() int {
	return t.n
}

// Contains
func (t *Trie[V]) Contains(key string) bool {
	if len(key) == 0 {
		return false
	}
	_, ok := t.Get(key)
	return ok
}

// Get
func (t *Trie[V]) Get(key string) (v V, ok bool) {
	if len(key) == 0 {
		return v, false
	}
	x := t.get(t.root, key, 0)
	if x == nil || !x.valid {
		return v, false
	}
	return x.val, true
}

func (t *Trie[V]) get(x *node[V], key string, d int) *node[V] {
	if x == nil || len(key) == 0 {
		return nil
	}
	c := key[d]
	if c < x.c {
		return t.get(x.left, key, d)
	} else if c > x.c {
		return t.get(x.right, key, d)
	} else if d < len(key)-1 {
		return t.get(x.mid, key, d+1)
	} else {
		return x
	}
}

// Put
func (t *Trie[V]) Put(key string, val V) {
	if len(key) == 0 {
		return
	}
	if !t.Contains(key) {
		t.n++
	}
	t.root = t.put(t.root, key, val, 0)
}

func (t *Trie[V]) put(x *node[V], key string, val V, d int) *node[V] {
	c := key[d]
	if x == nil {
		x = &node[V]{
			c: c,
		}
	}
	if c < x.c {
		x.left = t.put(x.left, key, val, d)
	} else if c > x.c {
		x.right = t.put(x.right, key, val, d)
	} else if d < len(key)-1 {
		x.mid = t.put(x.mid, key, val, d+1)
	} else {
		x.val = val
		x.valid = true
	}
	return x
}

// Remove
func (t *Trie[V]) Remove(key string) {
	if len(key) == 0 {
		return
	}
	t.root = t.remove(t.root, key, 0)
	t.n--
}

func (t *Trie[V]) remove(x *node[V], key string, d int) *node[V] {
	if x == nil {
		return nil
	}

	c := key[d]
	if c < x.c {
		x.left = t.remove(x.left, key, d)

	} else if c > x.c {
		x.right = t.remove(x.right, key, d)

	} else if d < len(key)-1 {
		x.mid = t.remove(x.mid, key, d+1)

	} else {
		x.valid = false
	}

	if x.isUnused() {
		return x.delete()
	}

	return x
}

func (t *Trie[V]) collect(x *node[V], prefix []byte, queue []string) []string {
	if x == nil {
		return queue
	}
	queue = t.collect(x.left, prefix, queue)
	if x.valid {
		queue = append(queue, string(append(prefix, x.c)))
	}
	queue = t.collect(x.mid, append(prefix, x.c), queue)
	return t.collect(x.right, prefix, queue)
}

func (t *Trie[V]) collectAll(x *node[V], prefix []byte, queue []string, value []V) ([]string, []V) {
	if x == nil {
		return queue, value
	}
	queue, value = t.collectAll(x.left, prefix, queue, value)
	if x.valid {
		queue = append(queue, string(append(prefix, x.c)))
		value = append(value, x.val)
	}
	queue, value = t.collectAll(x.mid, append(prefix, x.c), queue, value)
	return t.collectAll(x.right, prefix, queue, value)
}

// Keys
func (t *Trie[V]) Keys() (queue []string) {
	return t.collect(t.root, nil, queue)
}

// KeysWithPrefix
func (t *Trie[V]) KeysWithPrefix(prefix string) (queue []string) {
	if len(prefix) == 0 {
		return t.Keys()
	}
	x := t.get(t.root, prefix, 0)
	if x == nil {
		return nil
	}
	if x.valid {
		queue = []string{prefix}
	}
	return t.collect(x.mid, []byte(prefix), queue)
}

// MarshalJSON
func (t *Trie[T]) MarshalJSON() ([]byte, error) {
	keys, values := t.collectAll(t.root, nil, nil, nil)

	return base.MarshalJSON(base.GTreeJSON[string, T]{K: keys, V: values})
}

// UnmarshalJSON
func (t *Trie[T]) UnmarshalJSON(src []byte) error {
	var tmp base.GTreeJSON[string, T]
	if err := base.UnmarshalJSON(src, &tmp); err != nil {
		return err
	}

	// set
	for i, k := range tmp.K {
		t.Put(k, tmp.V[i])
	}
	return nil
}
