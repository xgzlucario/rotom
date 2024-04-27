package structx

import (
	"sync"

	"github.com/bytedance/sonic"
	rbtree "github.com/sakeven/RbTree"
)

// ZSet
type ZSet struct {
	sync.RWMutex
	m    map[string]float64
	tree *rbtree.Tree[float64, string]
}

// NewZSet
func NewZSet() *ZSet {
	return &ZSet{
		m:    map[string]float64{},
		tree: rbtree.NewTree[float64, string](),
	}
}

// Get
func (z *ZSet) Get(key string) (float64, bool) {
	z.RLock()
	defer z.RUnlock()
	s, ok := z.m[key]
	return s, ok
}

// Set upsert value by key.
func (z *ZSet) Set(key string, score float64) {
	z.Lock()
	z.set(key, score)
	z.Unlock()
}

func (z *ZSet) set(key string, score float64) {
	z.deleteNode(score, key)
	z.m[key] = score
	z.tree.Insert(score, key)
}

// deleteNode
func (z *ZSet) deleteNode(score float64, key string) bool {
	for it := z.tree.FindIt(score); it != nil; it = it.Next() {
		if it.Value == key {
			z.tree.Delete(it.Key)
			return true
		}
		if it.Key != score {
			return false
		}
	}
	return false
}

// Incr
func (z *ZSet) Incr(key string, incr float64) float64 {
	z.Lock()
	score, ok := z.m[key]
	if ok {
		z.deleteNode(score, key)
	}
	score += incr
	z.m[key] = score
	z.tree.Insert(score, key)
	z.Unlock()
	return score
}

// Delete
func (z *ZSet) Delete(key string) (s float64, ok bool) {
	z.Lock()
	score, ok := z.m[key]
	if ok {
		delete(z.m, key)
		z.deleteNode(score, key)
	}
	z.Unlock()
	return score, ok
}

// Len
func (z *ZSet) Len() int {
	z.RLock()
	defer z.RUnlock()
	return len(z.m)
}

// Iter iterate all elements by scores.
func (z *ZSet) Iter(f func(k string, s float64) bool) {
	z.RLock()
	defer z.RUnlock()

	for it := z.tree.Iterator(); it != nil; it = it.Next() {
		if f(it.Value, it.Key) {
			return
		}
	}
}

// MarshalJSON
func (z *ZSet) MarshalJSON() ([]byte, error) {
	return sonic.Marshal(z.m)
}

// UnmarshalJSON
func (z *ZSet) UnmarshalJSON(src []byte) error {
	var m map[string]float64

	if err := sonic.Unmarshal(src, &m); err != nil {
		return err
	}
	for k, s := range m {
		z.tree.Insert(s, k)
	}

	return nil
}
