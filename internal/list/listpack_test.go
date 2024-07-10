package list

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func genListPack(start, end int) *ListPack {
	lp := NewListPack()
	it := lp.NewIterator()
	for i := start; i < end; i++ {
		it.Insert(genKey(i))
	}
	return lp
}

func genKey(i int) string {
	return fmt.Sprintf("%08x", i)
}

// func TestListPack(t *testing.T) {
// 	assert := assert.New(t)
// 	const N = 1000

// 	t.Run("rpush/lpop", func(t *testing.T) {
// 		lp := NewListPack()
// 		for i := 0; i < N; i++ {
// 			assert.Equal(lp.Size(), i)
// 			lp.Insert(-1, genKey(i))
// 		}
// 		for i := 0; i < N; i++ {
// 			val, ok := lp.Remove(0)
// 			assert.Equal(val, genKey(i))
// 			assert.Equal(true, ok)
// 		}
// 	})

// 	t.Run("lpush/rpop", func(t *testing.T) {
// 		lp := NewListPack()
// 		for i := 0; i < N; i++ {
// 			assert.Equal(lp.Size(), i)
// 			lp.Insert(0, genKey(i))
// 		}
// 		for i := 0; i < N; i++ {
// 			val, ok := lp.Remove(-1)
// 			assert.Equal(val, genKey(i))
// 			assert.Equal(true, ok)
// 		}
// 	})

// 	t.Run("iterFront", func(t *testing.T) {
// 		lp := genListPack(0, N)

// 		// iter [0, -1]
// 		var i int
// 		lp.iterFront(0, -1, func(data []byte, _, _, _ int) bool {
// 			assert.Equal(string(data), genKey(i))
// 			i++
// 			return false
// 		})
// 		assert.Equal(i, N)

// 		// iter [0, N/2]
// 		i = 0
// 		lp.iterFront(0, N/2, func(data []byte, _, _, _ int) bool {
// 			assert.Equal(string(data), genKey(i))
// 			i++
// 			return false
// 		})
// 		assert.Equal(i, N/2)

// 		// iter [N/2, -1]
// 		i = 0
// 		lp.iterFront(N/2, -1, func(data []byte, _, _, _ int) bool {
// 			assert.Equal(string(data), genKey(i+N/2))
// 			i++
// 			return false
// 		})
// 		assert.Equal(i, N/2)
// 	})

// 	t.Run("iterBack", func(t *testing.T) {
// 		lp := genListPack(0, N)

// 		// iter [0, -1]
// 		var i int
// 		lp.iterBack(0, -1, func(data []byte, _, _, _ int) bool {
// 			assert.Equal(string(data), genKey(N-1-i))
// 			i++
// 			return false
// 		})
// 		assert.Equal(i, N)

// 		// iter [0, N/2]
// 		i = 0
// 		lp.iterBack(0, N/2, func(data []byte, _, _, _ int) bool {
// 			assert.Equal(string(data), genKey(N-1-i))
// 			i++
// 			return false
// 		})
// 		assert.Equal(i, N/2)

// 		// iter [N/2, -1]
// 		i = 0
// 		lp.iterBack(N/2, -1, func(data []byte, _, _, _ int) bool {
// 			assert.Equal(string(data), genKey(N/2-i-1))
// 			i++
// 			return false
// 		})
// 		assert.Equal(i, N/2)
// 	})

// 	t.Run("remove", func(t *testing.T) {
// 		lp := genListPack(0, N)

// 		val, ok := lp.Remove(N)
// 		assert.Equal(val, "")
// 		assert.Equal(false, ok)

// 		val, ok = lp.Remove(0)
// 		assert.Equal(val, genKey(0))
// 		assert.Equal(true, ok)

// 		res, ok := lp.Remove(0)
// 		assert.Equal(res, genKey(1))
// 		assert.Equal(true, ok)
// 	})

// 	t.Run("removeFirst", func(t *testing.T) {
// 		lp := genListPack(0, N)

// 		index, ok := lp.RemoveFirst(genKey(N))
// 		if index != 0 || ok {
// 			t.Error(ok, index, N)
// 		}

// 		index, ok = lp.RemoveFirst(genKey(0))
// 		assert.Equal(index, 0)
// 		assert.Equal(true, ok)

// 		res, ok := lp.Remove(0)
// 		assert.Equal(res, genKey(1))
// 		assert.Equal(true, ok)
// 	})

// 	t.Run("set", func(t *testing.T) {
// 		lp := genListPack(0, N)

// 		for i := 0; i < N; i++ {
// 			newKey := fmt.Sprintf("newkey-%d", i)
// 			ok := lp.Set(i, newKey)
// 			if !ok {
// 				t.Error(ok)
// 			}

// 			var val string
// 			lp.find(i, func(data []byte, index, _, _ int) {
// 				val = string(data)
// 			})
// 			if newKey != val {
// 				t.Error(ok, val, i)
// 			}
// 		}

// 		// set -1
// 		ok := lp.Set(-1, "last")
// 		if !ok {
// 			t.Error(ok)
// 		}

// 		var val string
// 		lp.find(lp.Size()-1, func(data []byte, index, _, _ int) {
// 			val = string(data)
// 		})
// 		if string("last") != val {
// 			t.Error(ok, val)
// 		}

// 		// out of bound
// 		ok = lp.Set(N+1, "newKey")
// 		assert.Equal(false, ok)
// 	})

// 	t.Run("insert", func(t *testing.T) {
// 		lp := genListPack(0, 10)

// 		// insert to head
// 		lp.Insert(0, "test0")
// 		val, ok := lp.Remove(0)
// 		assert.Equal(val, "test0")
// 		assert.Equal(ok, true)

// 		// insert to mid
// 		lp.Insert(5, "test1")
// 		val, ok = lp.Remove(5)
// 		assert.Equal(val, "test1")
// 		assert.Equal(ok, true)

// 		// insert to tail
// 		lp.Insert(-1, "test2")
// 		val, ok = lp.Remove(-1)
// 		assert.Equal(val, "test2")
// 		assert.Equal(ok, true)
// 	})

// 	t.Run("range", func(t *testing.T) {
// 		lp := genListPack(0, N)

// 		// range [0,-1]
// 		count := 0
// 		lp.Range(0, -1, func(data []byte, index int) (stop bool) {
// 			if string(data) != genKey(index) {
// 				t.Error(string(data), genKey(index))
// 			}
// 			count++
// 			return false
// 		})
// 		if count != N {
// 			t.Error(count, N)
// 		}

// 		// revrange [0,-1]
// 		count = 0
// 		lp.RevRange(0, -1, func(data []byte, index int) (stop bool) {
// 			if string(data) != genKey(N-index-1) {
// 				t.Error(string(data), genKey(N-index-1))
// 			}
// 			count++
// 			return false
// 		})
// 		if count != N {
// 			t.Error(count, N)
// 		}

// 		// range [0,N/2]
// 		count = 0
// 		lp.Range(0, N/2, func(data []byte, index int) (stop bool) {
// 			if index > N/2 {
// 				t.Error(index, N/2)
// 			}
// 			count++
// 			return false
// 		})
// 		if count != N/2 {
// 			t.Error(count, N)
// 		}
// 	})
// }

func TestIterator(t *testing.T) {
	assert := assert.New(t)
	ls := NewListPack()
	it := ls.NewIterator()

	it.Insert("003")
	it.Insert("002")
	it.Insert("001")

	data := it.Next()
	assert.Equal(string(data), "001")

	it.Insert("004") // [001, 004, 002, 003]

	data = it.Next()
	assert.Equal(string(data), "004")

	removed := it.RemoveNext()
	assert.Equal(string(removed), "002")

	removed = it.RemovePrev()
	assert.Equal(string(removed), "004")

	it.SeekEnd()

	assert.Equal(string(it.Prev()), "003")
	assert.Equal(string(it.Prev()), "001")
}

// func FuzzListPack(f *testing.F) {
// 	ls := NewListPack()
// 	vls := make([]string, 0, 4096)

// 	f.Fuzz(func(t *testing.T, rkey string) {
// 		assert := assert.New(t)

// 		switch rand.IntN(15) {
// 		// RPush
// 		case 0, 1, 2:
// 			ls.Insert(-1, rkey)
// 			vls = append(vls, rkey)

// 		// LPush
// 		case 3, 4, 5:
// 			ls.Insert(0, rkey)
// 			vls = append([]string{rkey}, vls...)

// 		// LPop
// 		case 6, 7:
// 			val, ok := ls.Remove(0)
// 			if len(vls) > 0 {
// 				valVls := vls[0]
// 				vls = vls[1:]
// 				assert.Equal(val, valVls)
// 				assert.Equal(true, ok)
// 			} else {
// 				assert.Equal(val, "")
// 				assert.Equal(false, ok)
// 			}

// 		// RPop
// 		case 8, 9:
// 			val, ok := ls.Remove(-1)
// 			if len(vls) > 0 {
// 				valVls := vls[len(vls)-1]
// 				vls = vls[:len(vls)-1]
// 				assert.Equal(val, valVls)
// 				assert.Equal(true, ok)
// 			} else {
// 				assert.Equal(val, "")
// 				assert.Equal(false, ok)
// 			}

// 		// Set
// 		case 10:
// 			if len(vls) > 0 {
// 				index := rand.IntN(len(vls))
// 				ok := ls.Set(index, rkey)
// 				assert.Equal(true, ok)
// 				vls[index] = rkey
// 			}

// 		// Remove
// 		case 12:
// 			if len(vls) > 0 {
// 				index := rand.IntN(len(vls))
// 				val, ok := ls.Remove(index)
// 				assert.Equal(val, vls[index])
// 				assert.Equal(true, ok)
// 				vls = append(vls[:index], vls[index+1:]...)
// 			}

// 		// Range
// 		case 13:
// 			if len(vls) <= 1 {
// 				break
// 			}
// 			end := rand.IntN(len(vls)-1) + 1
// 			start := rand.IntN(end)

// 			var count int
// 			ls.Range(start, end, func(data []byte, index int) (stop bool) {
// 				assert.Equal(b2s(data), vls[start+count])
// 				count++
// 				return false
// 			})
// 		}
// 	})
// }
