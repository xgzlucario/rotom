package structx

import (
	"sync"

	"github.com/bytedance/sonic"
	"github.com/cockroachdb/swiss"
	rbtree "github.com/sakeven/RbTree"
)

// ZSet
type ZSet struct {
	sync.RWMutex
	m    *swiss.Map[string, int64]
	tree *rbtree.Tree[int64, string]
}

// NewZSet
func NewZSet() *ZSet {
	return &ZSet{
		m:    swiss.New[string, int64](8),
		tree: rbtree.NewTree[int64, string](),
	}
}

// Get
func (z *ZSet) Get(key string) (score int64, ok bool) {
	z.RLock()
	score, ok = z.m.Get(key)
	z.RUnlock()
	return
}

// Set upsert value by key.
func (z *ZSet) Set(key string, score int64) {
	z.Lock()
	z.deleteNode(score, key)
	z.m.Put(key, score)
	z.tree.Insert(score, key)
	z.Unlock()
}

// deleteNode
func (z *ZSet) deleteNode(score int64, key string) bool {
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
func (z *ZSet) Incr(key string, incr int64) int64 {
	z.Lock()
	score, ok := z.m.Get(key)
	if ok {
		z.deleteNode(score, key)
	}
	score += incr
	z.m.Put(key, score)
	z.tree.Insert(score, key)
	z.Unlock()
	return score
}

// Delete
func (z *ZSet) Delete(key string) (s int64, ok bool) {
	z.Lock()
	score, ok := z.m.Get(key)
	if ok {
		z.m.Delete(key)
		z.deleteNode(score, key)
	}
	z.Unlock()
	return score, ok
}

// Len
func (z *ZSet) Len() int {
	z.RLock()
	defer z.RUnlock()
	return z.m.Len()
}

// Iter iterate all elements by scores.
func (z *ZSet) Iter(f func(k string, s int64) bool) {
	z.RLock()
	defer z.RUnlock()

	for it := z.tree.Iterator(); it != nil; it = it.Next() {
		if f(it.Value, it.Key) {
			return
		}
	}
}

type zentry struct {
	K []string
	S []int64
}

// MarshalJSON
func (z *ZSet) MarshalJSON() ([]byte, error) {
	data := zentry{
		K: make([]string, 0, z.m.Len()),
		S: make([]int64, 0, z.m.Len()),
	}
	z.m.All(func(key string, value int64) bool {
		data.K = append(data.K, key)
		data.S = append(data.S, value)
		return true
	})
	return sonic.Marshal(data)
}

// UnmarshalJSON
func (z *ZSet) UnmarshalJSON(src []byte) error {
	var data zentry
	if err := sonic.Unmarshal(src, &data); err != nil {
		return err
	}
	for i, k := range data.K {
		s := data.S[i]
		z.tree.Insert(s, k)
		z.m.Put(k, s)
	}

	return nil
}
