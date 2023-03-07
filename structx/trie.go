package structx

import (
	"github.com/xgzlucario/rotom/base"
)

// A Trie is a data structure that supports common prefix operations.
type Trie[V any] struct {
	n    int
	root *node[V]
}

type node[V any] struct {
	c                byte
	left, mid, right *node[V]
	val              V
	valid            bool
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

// New returns an empty trie.
func NewTrie[V any]() *Trie[V] {
	return &Trie[V]{}
}

// Size returns the size of the trie.
func (t *Trie[V]) Size() int {
	return t.n
}

// Contains returns whether this trie contains 'key'.
func (t *Trie[V]) Contains(key string) bool {
	if len(key) == 0 {
		return false
	}
	_, ok := t.Get(key)
	return ok
}

// Get returns the value associated with 'key'.
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

	}
	return x
}

// Put associates 'val' with 'key'.
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

// Remove removes the value associated with 'key', along with any nodes of the key that are no
// longer used.
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

// LongestPrefix returns the key that is the longest prefix of 'query'.
func (t *Trie[V]) LongestPrefix(query string) string {
	if len(query) == 0 {
		return ""
	}
	length := 0
	x := t.root
	i := 0
	for x != nil && i < len(query) {
		c := query[i]
		if c < x.c {
			x = x.left

		} else if c > x.c {
			x = x.right

		} else {
			i++
			if x.valid {
				length = i
			}
			x = x.mid
		}
	}
	return query[:length]
}

// collect key-value recursive
func (x *node[V]) collect(done *bool, prefix []byte, f func(string, V) bool) {
	if done == nil {
		done = new(bool)
	}
	if x == nil || *done {
		return
	}
	x.left.collect(done, prefix, f)

	if x.valid {
		if f(string(append(prefix, x.c)), x.val) {
			*done = true
			return
		}
	}
	x.mid.collect(done, append(prefix, x.c), f)
	x.right.collect(done, prefix, f)
}

// Keys return all keys.
func (t *Trie[T]) Keys() []string {
	keys := make([]string, 0, t.Size())
	t.root.collect(nil, nil, func(key string, _ T) bool {
		keys = append(keys, key)
		return false
	})
	return keys
}

// WalkPath traverses the tree based on prefixes.
func (t *Trie[T]) WalkPath(f func(string, T) bool, prefix ...string) {
	if len(prefix) == 0 {
		t.root.collect(nil, nil, f)

	} else {
		t.root.collect(nil, []byte(prefix[0]), f)
	}
}

// MarshalJSON
func (t *Trie[T]) MarshalJSON() ([]byte, error) {
	tmp := make(map[string]T, t.Size())

	t.root.collect(nil, nil, func(key string, val T) bool {
		tmp[key] = val
		return false
	})

	src, err := base.MarshalJSON(tmp)
	if err != nil {
		return nil, err
	}

	// compress
	return base.ZstdEncode(src), nil
}

// UnmarshalJSON
func (t *Trie[T]) UnmarshalJSON(src []byte) error {
	var tmp map[string]T

	// decompress
	src = base.ZstdEncode(src)

	if err := base.UnmarshalJSON(src, &tmp); err != nil {
		return err
	}

	// set
	for k, v := range tmp {
		t.Put(k, v)
	}
	return nil
}
