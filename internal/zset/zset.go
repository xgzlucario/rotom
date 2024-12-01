package zset

import (
	"cmp"
	"github.com/cockroachdb/swiss"
	"github.com/xgzlucario/rotom/internal/iface"
	"github.com/xgzlucario/rotom/internal/resp"

	"github.com/chen3feng/stl4go"
)

var (
	_ iface.ZSetI = (*ZSet)(nil)
)

type node struct {
	key   string
	score float64
}

func nodeCompare(a, b node) int {
	if a.score == b.score {
		return cmp.Compare(a.key, b.key)
	}
	return cmp.Compare(a.score, b.score)
}

type ZSet struct {
	m   *swiss.Map[string, float64]
	skl *stl4go.SkipList[node, struct{}]
}

func New() *ZSet {
	return &ZSet{
		m:   swiss.New[string, float64](8),
		skl: stl4go.NewSkipListFunc[node, struct{}](nodeCompare),
	}
}

func (z *ZSet) Get(key string) (float64, bool) {
	return z.m.Get(key)
}

func (z *ZSet) Set(key string, score float64) bool {
	old, ok := z.m.Get(key)
	if ok {
		// same
		if score == old {
			return false
		}
		z.skl.Remove(node{key, old})
	}
	z.m.Put(key, score)
	z.skl.Insert(node{key, score}, struct{}{})
	return !ok
}

func (z *ZSet) Remove(key string) bool {
	score, ok := z.m.Get(key)
	if !ok {
		return false
	}
	z.m.Delete(key)
	z.skl.Remove(node{key, score})
	return true
}

func (z *ZSet) PopMin() (key string, score float64) {
	z.skl.ForEachIf(func(n node, _ struct{}) bool {
		key = n.key
		score = n.score
		return false
	})
	z.m.Delete(key)
	z.skl.Remove(node{key, score})
	return
}

func (z *ZSet) Rank(key string) (index int) {
	index = -1
	_, ok := z.m.Get(key)
	if !ok {
		return
	}
	z.skl.ForEachIf(func(n node, _ struct{}) bool {
		index++
		return n.key != key
	})
	return
}

func (z *ZSet) Scan(fn func(key string, score float64)) {
	z.skl.ForEachIf(func(n node, _ struct{}) bool {
		fn(n.key, n.score)
		return true
	})
}

func (z *ZSet) Len() int {
	return z.m.Len()
}

func (z *ZSet) Encode(writer *resp.Writer) error {
	writer.WriteArray(z.Len())
	z.m.All(func(k string, s float64) bool {
		writer.WriteBulkString(k)
		writer.WriteFloat(s)
		return true
	})
	return nil
}

func (z *ZSet) Decode(reader *resp.Reader) error {
	//cmd, err := reader.ReadCommand()
	//if err != nil {
	//	return err
	//}
	//
	//
	//for range n {
	//	buf, err := reader.ReadBulk()
	//	if err != nil {
	//		return err
	//	}
	//	score, err := reader.ReadFloat()
	//	if err != nil {
	//		return err
	//	}
	//	key := string(buf)
	//	z.skl.Insert(node{key, score}, struct{}{})
	//	z.m.Put(key, score)
	//}
	return nil
}
