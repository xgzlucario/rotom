package structx

import (
	"github.com/bytedance/sonic"
	"github.com/xgzlucario/rotom/base"
)

// color of node
const (
	RED   = 0
	BLACK = 1
)

type rbnode[K base.Ordered, V any] struct {
	color               byte
	Key                 K
	Value               V
	left, right, parent *rbnode[K, V]
}

// Tree is a struct of red-black tree.
type RBTree[K base.Ordered, V any] struct {
	size int
	root *rbnode[K, V]
}

// NewRBTree
func NewRBTree[K base.Ordered, V any]() *RBTree[K, V] {
	return &RBTree[K, V]{}
}

// Find finds the node and return its value.
func (t *RBTree[K, V]) Find(key K) (res V) {
	n := t.findnode(key)
	if n != nil {
		return n.Value
	}
	return
}

// FindIt finds the node and return it as an iterator.
func (t *RBTree[K, V]) FindIt(key K) *rbnode[K, V] {
	return t.findnode(key)
}

// Empty checks whether the rbtree is empty.
func (t *RBTree[K, V]) Empty() bool {
	return t.root == nil
}

// Iterator creates the rbtree's iterator that points to the minmum node.
func (t *RBTree[K, V]) Iterator() *rbnode[K, V] {
	return minimum(t.root)
}

// Size returns the size of the rbtree.
func (t *RBTree[K, V]) Size() int {
	return t.size
}

// Clear destroys the rbtree.
func (t *RBTree[K, V]) Clear() {
	t.root = nil
	t.size = 0
}

// Insert inserts the key-value pair into the rbtree.
func (t *RBTree[K, V]) Insert(key K, value V) {
	x := t.root
	var y *rbnode[K, V]

	for x != nil {
		y = x
		if key < x.Key {
			x = x.left
		} else {
			x = x.right
		}
	}

	z := &rbnode[K, V]{parent: y, color: RED, Key: key, Value: value}
	t.size++

	if y == nil {
		z.color = BLACK
		t.root = z
		return
	} else if z.Key < y.Key {
		y.left = z
	} else {
		y.right = z
	}
	t.rbInsertFixup(z)

}

// Delete deletes the node by key
func (t *RBTree[K, V]) Delete(key K) {
	z := t.findnode(key)
	if z == nil {
		return
	}

	var x, y *rbnode[K, V]
	if z.left != nil && z.right != nil {
		y = successor(z)
	} else {
		y = z
	}

	if y.left != nil {
		x = y.left
	} else {
		x = y.right
	}

	xparent := y.parent
	if x != nil {
		x.parent = xparent
	}
	if y.parent == nil {
		t.root = x
	} else if y == y.parent.left {
		y.parent.left = x
	} else {
		y.parent.right = x
	}

	if y != z {
		z.Key = y.Key
		z.Value = y.Value
	}

	if y.color == BLACK {
		t.rbDeleteFixup(x, xparent)
	}
	t.size--
}

func (t *RBTree[K, V]) rbInsertFixup(z *rbnode[K, V]) {
	var y *rbnode[K, V]
	for z.parent != nil && z.parent.color == RED {
		if z.parent == z.parent.parent.left {
			y = z.parent.parent.right
			if y != nil && y.color == RED {
				z.parent.color = BLACK
				y.color = BLACK
				z.parent.parent.color = RED
				z = z.parent.parent
			} else {
				if z == z.parent.right {
					z = z.parent
					t.leftRotate(z)
				}
				z.parent.color = BLACK
				z.parent.parent.color = RED
				t.rightRotate(z.parent.parent)
			}
		} else {
			y = z.parent.parent.left
			if y != nil && y.color == RED {
				z.parent.color = BLACK
				y.color = BLACK
				z.parent.parent.color = RED
				z = z.parent.parent
			} else {
				if z == z.parent.left {
					z = z.parent
					t.rightRotate(z)
				}
				z.parent.color = BLACK
				z.parent.parent.color = RED
				t.leftRotate(z.parent.parent)
			}
		}
	}
	t.root.color = BLACK
}

func (t *RBTree[K, V]) rbDeleteFixup(x, parent *rbnode[K, V]) {
	var w *rbnode[K, V]

	for x != t.root && getColor(x) == BLACK {
		if x != nil {
			parent = x.parent
		}
		if x == parent.left {
			w = parent.right
			if w.color == RED {
				w.color = BLACK
				parent.color = RED
				t.leftRotate(parent)
				w = parent.right
			}
			if getColor(w.left) == BLACK && getColor(w.right) == BLACK {
				w.color = RED
				x = parent
			} else {
				if getColor(w.right) == BLACK {
					if w.left != nil {
						w.left.color = BLACK
					}
					w.color = RED
					t.rightRotate(w)
					w = parent.right
				}
				w.color = parent.color
				parent.color = BLACK
				if w.right != nil {
					w.right.color = BLACK
				}
				t.leftRotate(parent)
				x = t.root
			}
		} else {
			w = parent.left
			if w.color == RED {
				w.color = BLACK
				parent.color = RED
				t.rightRotate(parent)
				w = parent.left
			}
			if getColor(w.left) == BLACK && getColor(w.right) == BLACK {
				w.color = RED
				x = parent
			} else {
				if getColor(w.left) == BLACK {
					if w.right != nil {
						w.right.color = BLACK
					}
					w.color = RED
					t.leftRotate(w)
					w = parent.left
				}
				w.color = parent.color
				parent.color = BLACK
				if w.left != nil {
					w.left.color = BLACK
				}
				t.rightRotate(parent)
				x = t.root
			}
		}
	}
	if x != nil {
		x.color = BLACK
	}
}

func (t *RBTree[K, V]) leftRotate(x *rbnode[K, V]) {
	y := x.right
	x.right = y.left
	if y.left != nil {
		y.left.parent = x
	}
	y.parent = x.parent
	if x.parent == nil {
		t.root = y
	} else if x == x.parent.left {
		x.parent.left = y
	} else {
		x.parent.right = y
	}
	y.left = x
	x.parent = y
}

func (t *RBTree[K, V]) rightRotate(x *rbnode[K, V]) {
	y := x.left
	x.left = y.right
	if y.right != nil {
		y.right.parent = x
	}
	y.parent = x.parent
	if x.parent == nil {
		t.root = y
	} else if x == x.parent.right {
		x.parent.right = y
	} else {
		x.parent.left = y
	}
	y.right = x
	x.parent = y
}

// findnode finds the node by key and return it, if not exists return nil.
func (t *RBTree[K, V]) findnode(key K) *rbnode[K, V] {
	x := t.root
	for x != nil {
		if key < x.Key {
			x = x.left
		} else {
			if key == x.Key {
				return x
			}
			x = x.right
		}
	}
	return nil
}

// Next returns the node's successor as an iterator.
func (n *rbnode[K, V]) Next() *rbnode[K, V] {
	return successor(n)
}

// successor returns the successor of the node
func successor[K base.Ordered, V any](x *rbnode[K, V]) *rbnode[K, V] {
	if x.right != nil {
		return minimum(x.right)
	}
	y := x.parent
	for y != nil && x == y.right {
		x = y
		y = x.parent
	}
	return y
}

// getColor gets color of the node.
func getColor[K base.Ordered, V any](n *rbnode[K, V]) byte {
	if n == nil {
		return BLACK
	}
	return n.color
}

// minimum finds the minimum node of subtree n.
func minimum[K base.Ordered, V any](n *rbnode[K, V]) *rbnode[K, V] {
	for n.left != nil {
		n = n.left
	}
	return n
}

// MarshalJSON
func (b *RBTree[K, V]) MarshalJSON() ([]byte, error) {
	tmp := base.GTreeJSON[K, V]{
		K: make([]K, 0, b.Size()),
		V: make([]V, 0, b.Size()),
	}
	for f := b.Iterator(); f != nil; f = f.Next() {
		tmp.K = append(tmp.K, f.Key)
		tmp.V = append(tmp.V, f.Value)
	}

	return sonic.Marshal(tmp)
}

// UnmarshalJSON
func (b *RBTree[K, V]) UnmarshalJSON(src []byte) error {
	var tmp base.GTreeJSON[K, V]
	if err := sonic.Unmarshal(src, b); err != nil {
		return err
	}
	b = NewRBTree[K, V]()
	for i, k := range tmp.K {
		b.Insert(k, tmp.V[i])
	}

	return nil
}
