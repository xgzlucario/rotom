package list

// import (
// 	"fmt"
// 	"math/rand/v2"
// 	"strconv"
// 	"testing"

// 	"github.com/stretchr/testify/assert"
// )

// func genList(start, end int) *QuickList {
// 	lp := New()
// 	for i := start; i < end; i++ {
// 		lp.RPush(genKey(i))
// 	}
// 	return lp
// }

// func TestList(t *testing.T) {
// 	const N = 1000
// 	assert := assert.New(t)
// 	SetMaxListPackSize(128)

// 	t.Run("rpush", func(t *testing.T) {
// 		ls := New()
// 		for i := 0; i < N; i++ {
// 			assert.Equal(ls.Size(), i)
// 			ls.RPush(genKey(i))
// 		}
// 		for i := 0; i < N; i++ {
// 			v, ok := ls.Index(i)
// 			assert.Equal(genKey(i), v)
// 			assert.Equal(true, ok)
// 		}
// 		// check each node length
// 		for cur := ls.head; cur != nil; cur = cur.next {
// 			assert.LessOrEqual(len(cur.data), maxListPackSize)
// 		}
// 	})

// 	t.Run("lpush", func(t *testing.T) {
// 		ls := New()
// 		for i := 0; i < N; i++ {
// 			assert.Equal(ls.Size(), i)
// 			ls.LPush(genKey(i))
// 		}
// 		for i := 0; i < N; i++ {
// 			v, ok := ls.Index(N - 1 - i)
// 			assert.Equal(genKey(i), v)
// 			assert.Equal(true, ok)
// 		}
// 		// check each node length
// 		for cur := ls.head; cur != nil; cur = cur.next {
// 			assert.LessOrEqual(len(cur.data), maxListPackSize)
// 		}
// 	})

// 	t.Run("lpop", func(t *testing.T) {
// 		ls := genList(0, N)
// 		for i := 0; i < N; i++ {
// 			assert.Equal(ls.Size(), N-i)
// 			key, ok := ls.LPop()
// 			assert.Equal(key, genKey(i))
// 			assert.Equal(true, ok)
// 		}
// 		// pop empty list
// 		key, ok := ls.LPop()
// 		assert.Equal(key, "")
// 		assert.Equal(false, ok)
// 	})

// 	t.Run("rpop", func(t *testing.T) {
// 		ls := genList(0, N)
// 		for i := 0; i < N; i++ {
// 			assert.Equal(ls.Size(), N-i)
// 			key, ok := ls.RPop()
// 			assert.Equal(key, genKey(N-i-1))
// 			assert.Equal(true, ok)
// 		}
// 		// pop empty list
// 		key, ok := ls.RPop()
// 		assert.Equal(key, "")
// 		assert.Equal(false, ok)
// 	})

// 	t.Run("len", func(t *testing.T) {
// 		ls := New()
// 		for i := 0; i < N; i++ {
// 			ls.RPush(genKey(i))
// 			assert.Equal(ls.Size(), i+1)
// 		}
// 	})

// 	t.Run("set", func(t *testing.T) {
// 		ls := genList(0, N)
// 		for i := 0; i < N; i++ {
// 			newK := fmt.Sprintf("newkk-%x", i)
// 			ok := ls.Set(i, newK)
// 			assert.Equal(true, ok)
// 		}
// 		var count int
// 		ls.Range(0, -1, func(b []byte) bool {
// 			targetK := fmt.Sprintf("newkk-%x", count)
// 			assert.Equal(string(b), targetK)
// 			count++
// 			return false
// 		})
// 		assert.Equal(N, count)

// 		ok := ls.Set(N+1, "new")
// 		assert.Equal(false, ok)
// 	})

// 	t.Run("remove", func(t *testing.T) {
// 		ls := genList(0, N)
// 		for i := 0; i < N-1; i++ {
// 			val, ok := ls.Remove(0)
// 			assert.Equal(val, genKey(i))
// 			assert.Equal(true, ok)

// 			val, ok = ls.Index(0)
// 			assert.Equal(val, genKey(i+1))
// 			assert.Equal(true, ok)
// 		}

// 		assert.Equal(ls.head.Size(), 0)
// 		// only has 2 nodes.
// 		assert.Equal(ls.head.next, ls.tail)
// 		assert.Equal(ls.tail.Size(), 1)

// 		val, ok := ls.tail.Remove(-1)
// 		assert.Equal(val, genKey(N-1))
// 		assert.Equal(true, ok)
// 	})

// 	t.Run("removeFirst", func(t *testing.T) {
// 		ls := genList(0, N)

// 		// remove not exist item.
// 		index, ok := ls.RemoveFirst("none")
// 		if index != 0 {
// 			t.Error(index, 0)
// 		}
// 		if ok {
// 			t.Error(ok)
// 		}

// 		for i := 0; i < N-1; i++ {
// 			// same as LPop
// 			index, ok := ls.RemoveFirst(genKey(i))
// 			if index != 0 {
// 				t.Error(index, i)
// 			}
// 			if !ok {
// 				t.Error(ok)
// 			}

// 			val, ok := ls.Index(0)
// 			if val != genKey(i+1) {
// 				t.Error(val, genKey(i+1))
// 			}
// 			if !ok {
// 				t.Error(ok)
// 			}
// 		}

// 		assert.Equal(ls.head.Size(), 0)

// 		// only has 2 nodes.
// 		assert.Equal(ls.head.next, ls.tail)
// 		assert.Equal(ls.tail.Size(), 1)

// 		val, ok := ls.tail.Remove(-1)
// 		assert.Equal(val, genKey(N-1))
// 		assert.Equal(true, ok)
// 	})

// 	t.Run("range", func(t *testing.T) {
// 		ls := New()
// 		ls.Range(1, 2, func(s []byte) bool {
// 			panic("should not call")
// 		})
// 		ls = genList(0, N)

// 		var count int
// 		ls.Range(0, -1, func(s []byte) bool {
// 			assert.Equal(string(s), genKey(count))
// 			count++
// 			return false
// 		})
// 		assert.Equal(count, N)

// 		ls.Range(1, 1, func(s []byte) bool {
// 			panic("should not call")
// 		})
// 		ls.Range(-1, -1, func(s []byte) bool {
// 			panic("should not call")
// 		})
// 	})

// 	t.Run("revrange", func(t *testing.T) {
// 		ls := New()
// 		ls.RevRange(1, 2, func(s []byte) bool {
// 			panic("should not call")
// 		})
// 		ls = genList(0, N)

// 		var count int
// 		ls.RevRange(0, -1, func(s []byte) bool {
// 			assert.Equal(string(s), genKey(N-count-1))
// 			count++
// 			return false
// 		})
// 		assert.Equal(count, N)

// 		ls.RevRange(1, 1, func(s []byte) bool {
// 			panic("should not call")
// 		})
// 		ls.RevRange(-1, -1, func(s []byte) bool {
// 			panic("should not call")
// 		})
// 	})
// }

// func FuzzList(f *testing.F) {
// 	ls := New()
// 	vls := make([]string, 0, 4096)

// 	f.Fuzz(func(t *testing.T, key string) {
// 		assert := assert.New(t)

// 		switch rand.IntN(15) {
// 		// RPush
// 		case 0, 1, 2:
// 			k := strconv.Itoa(rand.Int())
// 			ls.RPush(k)
// 			vls = append(vls, k)

// 		// LPush
// 		case 3, 4, 5:
// 			k := strconv.Itoa(rand.Int())
// 			ls.LPush(k)
// 			vls = append([]string{k}, vls...)

// 		// LPop
// 		case 6, 7:
// 			val, ok := ls.LPop()
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
// 			val, ok := ls.RPop()
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
// 				randKey := fmt.Sprintf("%d", rand.Uint32())
// 				ok := ls.Set(index, randKey)
// 				assert.Equal(true, ok)
// 				vls[index] = randKey
// 			}

// 		// Index
// 		case 11:
// 			if len(vls) > 0 {
// 				index := rand.IntN(len(vls))
// 				val, ok := ls.Index(index)
// 				vlsVal := vls[index]
// 				assert.Equal(val, vlsVal)
// 				assert.Equal(true, ok)
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
// 			if len(vls) > 0 {
// 				end := rand.IntN(len(vls))
// 				if end == 0 {
// 					return
// 				}
// 				start := rand.IntN(end)

// 				var count int
// 				ls.Range(start, end, func(data []byte) bool {
// 					assert.Equal(b2s(data), vls[start+count])
// 					count++
// 					return false
// 				})
// 			}
// 		}
// 	})
// }
