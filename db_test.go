package rotom

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xgzlucario/rotom/codeman"
)

var (
	nilBytes   []byte
	nilStrings []string
)

func createDB() (*DB, error) {
	options := DefaultOptions
	options.ShardCount = 4
	options.DirPath = fmt.Sprintf("tmp-%x", time.Now().UnixNano())
	return Open(options)
}

func TestDB(t *testing.T) {
	assert := assert.New(t)
	const N = 5000
	db, err := createDB()
	assert.Nil(err)

	// Test db operations
	for i := 0; i < N; i++ {
		key := strconv.Itoa(i)
		val := []byte(strconv.Itoa(i))
		db.Set("set-"+key, val)
		db.SetEx("ttl-"+key, val, time.Minute)
		db.SetEx("expired-"+key, val, time.Second)
		db.SetTx("invalid-"+key, val, -1)
	}
	time.Sleep(time.Second * 2)

	// Test get.
	var ts int64
	now := time.Now().UnixNano()

	for i := 0; i < N; i++ {
		key := strconv.Itoa(i)
		// set
		val, _, err := db.Get("set-" + key)
		assert.Nil(err)
		assert.Equal(val, []byte(key))
		// ttl
		val, ts, err = db.Get("ttl-" + key)
		assert.Nil(err)
		assert.Equal(val, []byte(key))
		assert.True(ts > now)
		// expired
		val, ts, err = db.Get("expired-" + key)
		assert.Equal(val, nilBytes)
		assert.Equal(err, ErrKeyNotFound)
		assert.Equal(ts, int64(0))
		// invalid
		val, ts, err = db.Get("invalid-" + key)
		assert.Equal(val, nilBytes)
		assert.Equal(err, ErrKeyNotFound)
		assert.Equal(ts, int64(0))
	}

	// Scan
	var count int
	db.Scan(func(key, val []byte, ts int64) bool {
		count++
		return true
	})
	assert.Equal(count, N*2)
	assert.Equal(int(db.Len()), N*3)

	// GC
	db.GC()
	count = 0
	db.Scan(func(key, val []byte, ts int64) bool {
		count++
		return true
	})
	assert.Equal(count, N*2)
	assert.Equal(int(db.Len()), N*2)

	// Error
	val, _, err := db.Get("map")
	assert.Equal(val, nilBytes)
	assert.Equal(err, ErrKeyNotFound)

	val, _, err = db.Get("none")
	assert.Equal(val, nilBytes)
	assert.Equal(err, ErrKeyNotFound)

	// Remove
	n := db.Remove("set-1", "set-2", "set-3")
	assert.Equal(n, 3)

	// close
	assert.Nil(db.Close())
	assert.Equal(db.Close(), ErrDatabaseClosed)

	// Load Success
	_, err = Open(db.GetOptions())
	assert.Nil(err)

	t.Run("setTTL", func(t *testing.T) {
		db, err := createDB()
		assert.Nil(err)

		db.HSet("hmap", "k", []byte("v"))
		n := db.Remove("hmap")
		assert.Equal(n, 1)

		assert.False(db.SetTTL("h", -1))

		ts := time.Now().Add(time.Minute).UnixNano()
		for i := 0; i < 100; i++ {
			k := fmt.Sprintf("%08d", i)
			db.SetTx(k, []byte(k), ts)
		}
		// set ttl
		for i := 0; i < 100; i++ {
			k := fmt.Sprintf("%08d", i)
			assert.True(db.SetTTL(k, 0))
		}

		db.Close()
		db, _ = Open(db.GetOptions())

		// check ttl
		for i := 0; i < 100; i++ {
			k := fmt.Sprintf("%08d", i)
			v, ts, err := db.Get(k)
			assert.Equal(string(v), k)
			assert.Equal(int64(0), ts)
			assert.Nil(err)
		}
	})
}

func TestHmap(t *testing.T) {
	assert := assert.New(t)
	db, err := createDB()
	assert.Nil(err)
	defer db.Close()

	check := func() {
		for i := 0; i < 8000; i++ {
			mapkey := "map" + strconv.Itoa(i%100)
			key := "key" + strconv.Itoa(i)
			val := []byte(strconv.Itoa(i))

			// HGet
			res, err := db.HGet(mapkey, key)
			assert.Nil(err)
			assert.Equal(res, val)

			// HLen
			num, err := db.HLen(mapkey)
			assert.Nil(err)

			// HKeys
			keys, err := db.HKeys(mapkey)
			assert.Nil(err)
			assert.Equal(len(keys), num)
		}
	}

	for i := 0; i < 10000; i++ {
		mapkey := "map" + strconv.Itoa(i%100)
		key := "key" + strconv.Itoa(i)
		val := []byte(strconv.Itoa(i))

		// HSet
		err := db.HSet(mapkey, key, val)
		assert.Nil(err)

		if i > 8000 {
			// HRemove
			n, err := db.HRemove(mapkey, key)
			assert.Nil(err)
			assert.Equal(n, 1)
		}
	}

	check()

	// reload
	db.Close()
	db, err = Open(db.GetOptions())
	assert.Nil(err)

	check()

	// shrink and reload
	db.Shrink()
	db.Close()
	_, err = Open(db.GetOptions())
	assert.Nil(err)

	check()

	// Error
	db.LPush("fake", "123")

	err = db.HSet("fake", "a", []byte("b"))
	assert.ErrorContains(err, ErrWrongType.Error())

	res, err := db.HLen("fake")
	assert.Equal(res, 0)
	assert.ErrorContains(err, ErrWrongType.Error())

	m, err := db.HKeys("fake")
	assert.Equal(m, nilStrings)
	assert.ErrorContains(err, ErrWrongType.Error())

	n, err := db.HRemove("fake", "foo")
	assert.Equal(n, 0)
	assert.ErrorContains(err, ErrWrongType.Error())

	db.HSet("map", "m1", []byte("m2"))
	{
		res, err := db.HGet("fake", "none")
		assert.Nil(res)
		assert.ErrorContains(err, ErrWrongType.Error())
	}
	{
		res, err := db.HGet("map", "none")
		assert.Nil(res)
		assert.Equal(err, ErrFieldNotFound)
	}
}

func randString() string {
	return fmt.Sprintf("%08x", rand.Uint32())
}

func TestList(t *testing.T) {
	assert := assert.New(t)
	db, err := createDB()
	assert.Nil(err)

	for i := 0; i < 5000; i++ {
		key := "list" + strconv.Itoa(i/1000)
		val := randString()

		switch i % 3 {
		case 0:
			assert.Nil(db.RPush(key, val))
		case 1:
			assert.Nil(db.LPush(key, val))
			// check
			res, err := db.LIndex(key, 0)
			assert.Nil(err)
			assert.Equal(res, val)
		case 2:
			newKey := fmt.Sprintf("reset%d", i)
			ok, _ := db.LSet(key, 0, newKey)
			// check
			res, err := db.LIndex(key, 0)
			if errors.Is(err, ErrIndexOutOfRange) {
				assert.Equal(res, "")
				assert.False(ok)
			} else {
				assert.Nil(err)
				assert.Equal(res, newKey)
				assert.True(ok)
			}
		}

		if i > 4000 {
			switch i % 3 {
			case 0:
				res, err := db.RPop(key)
				assert.Nil(err)
				assert.Equal(res, val)
			case 1:
				res, err := db.LPop(key)
				assert.Nil(err)
				assert.Equal(res, val)
			}
		}

		num, err := db.LLen(key)
		assert.Nil(err)
		var count int
		err = db.LRange(key, 0, -1, func(s string) (stop bool) {
			count++
			return false
		})
		assert.Nil(err)
		assert.Equal(count, num)
	}

	// Error
	db.HSet("map", "key", []byte("value"))

	err = db.LPush("map", "1")
	assert.ErrorContains(err, ErrWrongType.Error())

	err = db.RPush("map", "1")
	assert.ErrorContains(err, ErrWrongType.Error())

	_, err = db.LSet("map", 1, "newKey")
	assert.ErrorContains(err, ErrWrongType.Error())

	err = db.LRange("map", 0, -1, func(s string) (stop bool) {
		return false
	})
	assert.ErrorContains(err, ErrWrongType.Error())

	res, err := db.LPop("map")
	assert.Equal(res, "")
	assert.ErrorContains(err, ErrWrongType.Error())

	res, err = db.RPop("map")
	assert.Equal(res, "")
	assert.ErrorContains(err, ErrWrongType.Error())

	s, err := db.LIndex("map", 1)
	assert.Equal(s, "")
	assert.ErrorContains(err, ErrWrongType.Error())

	n, err := db.LLen("map")
	assert.Equal(n, 0)
	assert.ErrorContains(err, ErrWrongType.Error())

	// empty list
	{
		db.RPush("list", "1")
		db.RPop("list")

		res, err = db.LPop("list")
		assert.Equal(res, "")
		assert.Equal(err, ErrEmptyList)

		res, err = db.RPop("list")
		assert.Equal(res, "")
		assert.Equal(err, ErrEmptyList)

		res, err = db.LIndex("list", 9)
		assert.Equal(res, "")
		assert.Equal(err, ErrIndexOutOfRange)

		for i := 0; i < 100; i++ {
			db.RPush("list", randString())
		}
	}

	// reload
	db.Close()
	db, err = Open(db.GetOptions())
	assert.Nil(err)

	// shrink and reload
	db.Shrink()
	db.Close()
	_, err = Open(db.GetOptions())
	assert.Nil(err)

	t.Run("lpop", func(t *testing.T) {
		db, err := createDB()
		assert.Nil(err)

		for i := 0; i < 1000; i++ {
			db.RPush("ls", strconv.Itoa(i))
		}
		for i := 0; i < 1000; i++ {
			v, err := db.LPop("ls")
			assert.Equal(v, strconv.Itoa(i))
			assert.Nil(err)
		}
	})

	t.Run("rpop", func(t *testing.T) {
		db, err := createDB()
		assert.Nil(err)

		for i := 0; i < 1000; i++ {
			db.LPush("ls", strconv.Itoa(i))
		}
		for i := 0; i < 1000; i++ {
			v, err := db.RPop("ls")
			assert.Equal(v, strconv.Itoa(i))
			assert.Nil(err)
		}
	})
}

func TestSet(t *testing.T) {
	assert := assert.New(t)
	db, err := createDB()
	assert.Nil(err)

	// SAdd
	for i := 0; i < 1000; i++ {
		n, err := db.SAdd("set"+strconv.Itoa(i/100), strconv.Itoa(i))
		assert.Equal(n, 1)
		assert.Nil(err)
	}

	// SHas
	for i := 500; i < 1500; i++ {
		ok, err := db.SHas("set"+strconv.Itoa(i/100), strconv.Itoa(i))
		assert.Nil(err)
		assert.Equal(ok, i < 1000)
	}

	// SRemove
	for i := 0; i < 1000; i++ {
		key := "set" + strconv.Itoa(i/100)

		if i%2 == 0 {
			assert.Nil(db.SRemove(key, strconv.Itoa(i)))
		}

		err = db.SRemove(key, "none")
		assert.Nil(err)

		// SCard SMembers
		n, err1 := db.SCard(key)
		m, err2 := db.SMembers(key)
		assert.Nil(err1)
		assert.Nil(err2)
		assert.Equal(n, len(m))
	}

	// Union
	for i := 0; i < 1000; i++ {
		// Add random data
		for i := 0; i < 20; i++ {
			stri := strconv.Itoa(i)
			db.SAdd("a"+stri, randString())
			db.SAdd("b"+stri, randString())
		}
		stri := strconv.Itoa(i)

		err = db.SUnion("union"+stri, "a"+stri, "b"+stri)
		assert.Nil(err)

		err = db.SInter("inter"+stri, "a"+stri, "b"+stri)
		assert.Nil(err)

		err = db.SDiff("diff"+stri, "a"+stri, "b"+stri)
		assert.Nil(err)

		// diff + inter = union
		db.SUnion("res"+stri, "inter"+stri, "diff"+stri)

		m1, err1 := db.SMembers("union" + stri)
		assert.Nil(err1)
		m2, err2 := db.SMembers("res" + stri)
		assert.Nil(err2)
		assert.ElementsMatch(m1, m2)
	}

	// Error
	db.SAdd("set", "1")

	db.HSet("map", "key", []byte("1"))
	n, err := db.SAdd("map", "1")
	assert.Equal(n, 0)
	assert.ErrorContains(err, ErrWrongType.Error())

	ok, err := db.SHas("map", "1")
	assert.False(ok)
	assert.ErrorContains(err, ErrWrongType.Error())

	err = db.SRemove("map", "1")
	assert.ErrorContains(err, ErrWrongType.Error())

	n, err = db.SCard("map")
	assert.Equal(n, 0)
	assert.ErrorContains(err, ErrWrongType.Error())

	m, err := db.SMembers("map")
	assert.Equal(m, nilStrings)
	assert.ErrorContains(err, ErrWrongType.Error())

	err = db.SUnion("", "map", "set")
	assert.ErrorContains(err, ErrWrongType.Error())
	err = db.SUnion("", "set", "map")
	assert.ErrorContains(err, ErrWrongType.Error())

	err = db.SDiff("", "map", "set")
	assert.ErrorContains(err, ErrWrongType.Error())
	err = db.SDiff("", "set", "map")
	assert.ErrorContains(err, ErrWrongType.Error())

	err = db.SInter("", "map", "set")
	assert.ErrorContains(err, ErrWrongType.Error())
	err = db.SInter("", "set", "map")
	assert.ErrorContains(err, ErrWrongType.Error())

	// reload
	db.Close()
	db, err = Open(db.GetOptions())
	assert.Nil(err)

	// shrink and reload
	db.Shrink()
	db.Close()
	_, err = Open(db.GetOptions())
	assert.Nil(err)
}

func TestBitmap(t *testing.T) {
	assert := assert.New(t)
	db, err := createDB()
	assert.Nil(err)

	for i := 0; i < 1000; i++ {
		key := strconv.Itoa(i / 100)

		n, err := db.BitSet(key, true, uint32(i))
		assert.Nil(err)
		assert.Equal(n, 1)

		n, err = db.BitSet(key, false, uint32(i))
		assert.Nil(err)
		assert.Equal(n, 1)

		n, err = db.BitSet(key, true, uint32(i))
		assert.Nil(err)
		assert.Equal(n, 1)

		ok, err := db.BitTest(key, uint32(i))
		assert.True(ok)
		assert.Nil(err)

		// Error
		db.BitSet("my-bitset", true, 1)
		db.LPush("none", "123")

		n, err = db.BitSet("none", true, uint32(i))
		assert.Equal(n, 0)
		assert.ErrorContains(err, ErrWrongType.Error())

		ok, err = db.BitTest("none", uint32(i))
		assert.False(ok)
		assert.ErrorContains(err, ErrWrongType.Error())

		err = db.BitFlip("none", uint32(i), uint32(i+1))
		assert.ErrorContains(err, ErrWrongType.Error())

		m, err := db.BitArray("none")
		assert.Nil(m)
		assert.ErrorContains(err, ErrWrongType.Error())

		num, err := db.BitCount("none")
		assert.Equal(num, uint64(0))
		assert.ErrorContains(err, ErrWrongType.Error())

		err = db.BitAnd("", "none", "my-bitset")
		assert.ErrorContains(err, ErrWrongType.Error())
		err = db.BitAnd("", "my-bitset", "none")
		assert.ErrorContains(err, ErrWrongType.Error())

		err = db.BitOr("", "none", "my-bitset")
		assert.ErrorContains(err, ErrWrongType.Error())
		err = db.BitOr("", "my-bitset", "none")
		assert.ErrorContains(err, ErrWrongType.Error())

		err = db.BitXor("", "none", "my-bitset")
		assert.ErrorContains(err, ErrWrongType.Error())
		err = db.BitXor("", "my-bitset", "none")
		assert.ErrorContains(err, ErrWrongType.Error())
	}

	for i := 0; i < 1000; i++ {
		// Add random data
		for i := 0; i < 20; i++ {
			stri := strconv.Itoa(i)
			db.BitSet("a"+stri, true, rand.Uint32())
			db.BitSet("b"+stri, true, rand.Uint32())
		}
		stri := strconv.Itoa(i)

		err = db.BitOr("or"+stri, "a"+stri, "b"+stri)
		assert.Nil(err)

		err = db.BitAnd("and"+stri, "a"+stri, "b"+stri)
		assert.Nil(err)

		err = db.BitXor("xor"+stri, "a"+stri, "b"+stri)
		assert.Nil(err)

		// xor + and = or
		db.BitOr("res"+stri, "and"+stri, "xor"+stri)

		m1, err1 := db.BitArray("or" + stri)
		assert.Nil(err1)
		n1, errn1 := db.BitCount("or" + stri)
		assert.Nil(errn1)
		assert.Equal(uint64(len(m1)), n1)

		m2, err2 := db.BitArray("res" + stri)
		assert.Nil(err2)
		assert.ElementsMatch(m1, m2)
	}

	// flip
	for i := 0; i < 1000; i++ {
		db.BitSet("bf", true, uint32(i))
	}
	db.BitFlip("bf", 500, 1500)
	for i := 500; i < 1000; i++ {
		ok, _ := db.BitTest("bf", uint32(i))
		assert.False(ok)
	}
	for i := 1000; i < 1500; i++ {
		ok, _ := db.BitTest("bf", uint32(i))
		assert.True(ok)
	}

	// reload
	db.Close()
	db, err = Open(db.GetOptions())
	assert.Nil(err)

	// shrink and reload
	db.Shrink()
	db.Close()
	_, err = Open(db.GetOptions())
	assert.Nil(err)
}

func TestZSet(t *testing.T) {
	assert := assert.New(t)
	db, err := createDB()
	assert.Nil(err)

	genKey := func(i int) string { return fmt.Sprintf("key-%06d", i) }

	// ZAdd
	for i := 0; i < 1000; i++ {
		err := db.ZAdd("zset", genKey(i), float64(i))
		assert.Nil(err)

		// card
		n, err := db.ZCard("zset")
		assert.Nil(err)
		assert.Equal(n, i+1)
	}

	check := func() {
		// exist
		for i := 0; i < 1000; i++ {
			// card
			n, err := db.ZCard("zset")
			assert.Nil(err)
			assert.Equal(n, 1000)

			// zget
			score, err := db.ZGet("zset", genKey(i))
			assert.Nil(err)
			assert.Equal(score, float64(i))
		}

		// not exist
		for i := 1000; i < 2000; i++ {
			score, err := db.ZGet("zset", genKey(i))
			assert.Equal(err, ErrKeyNotFound)
			assert.Equal(score, float64(0))
		}

		// iter
		count := 0
		err = db.ZIter("zset", func(key string, score float64) bool {
			count++
			return count >= 1000
		})
		assert.Nil(err)
		assert.Equal(count, 1000)
	}

	check()

	// Reload
	db.Close()
	db, err = Open(db.GetOptions())
	assert.Nil(err)

	check()

	// ZIncr
	for i := 0; i < 1000; i++ {
		num, err := db.ZIncr("zset", genKey(i), 3)
		assert.Nil(err)
		assert.Equal(num, float64(i+3))
	}
	for i := 3000; i < 4000; i++ {
		num, err := db.ZIncr("zset", genKey(i), 3)
		assert.Nil(err)
		assert.Equal(num, float64(3))
	}

	// ZRemove
	for i := 0; i < 800; i++ {
		err := db.ZRemove("zset", genKey(i))
		assert.Nil(err)
	}

	for i := 5000; i < 6000; i++ {
		err := db.ZRemove("zset", genKey(i))
		assert.Nil(err)
	}

	// reload
	db.Close()
	db, err = Open(db.GetOptions())
	assert.Nil(err)

	// shrink and reload
	db.Shrink()
	db.Close()
	db, err = Open(db.GetOptions())
	assert.Nil(err)

	// Test error
	db.SAdd("set", "1")

	n, err := db.ZGet("set", "1")
	assert.Equal(n, float64(0))
	assert.ErrorContains(err, ErrWrongType.Error())

	err = db.ZIter("set", func(key string, score float64) bool {
		return false
	})
	assert.ErrorContains(err, ErrWrongType.Error())

	err = db.ZAdd("set", "key", 1)
	assert.ErrorContains(err, ErrWrongType.Error())

	_, err = db.ZIncr("set", "key", 1)
	assert.ErrorContains(err, ErrWrongType.Error())

	err = db.ZRemove("set", "key")
	assert.ErrorContains(err, ErrWrongType.Error())

	_, err = db.ZCard("set")
	assert.ErrorContains(err, ErrWrongType.Error())
}

func TestInvalidCodec(t *testing.T) {
	assert := assert.New(t)

	// read args.
	codec := newCodec(OpSetTx).Bool(true)
	reader := codeman.NewReader(codec.Content())

	n := reader.Uint32()
	assert.Equal(uint64(n), uint64(OpSetTx))

	assert.Equal(true, reader.Bool())

	assert.Panics(func() {
		reader.StrSlice()
	})
	assert.Panics(func() {
		reader.Byte()
	})
	assert.Panics(func() {
		reader.RawBytes()
	})
}

func TestRace(t *testing.T) {
	assert := assert.New(t)

	t.Run("checkOptions", func(t *testing.T) {
		options := DefaultOptions
		options.DirPath = ""
		_, err := Open(options)
		assert.NotNil(err)

		options.DirPath = "test1"
		options.ShardCount = 0
		_, err = Open(options)
		assert.NotNil(err)
	})

	t.Run("open-wal", func(t *testing.T) {
		options := DefaultOptions
		options.DirPath = "README.md"
		_, err := Open(options)
		assert.NotNil(err)
	})
}

func TestUnmarshalError(t *testing.T) {
	assert := assert.New(t)

	for _, types := range []int64{TypeMap, TypeList, TypeSet, TypeZSet, TypeBitmap} {
		db, err := createDB()
		assert.Nil(err)

		// unmarshal error.
		db.encode(newCodec(OpSetObject).Int(types).Str("key").Bytes([]byte("error")))
		db.Close()
		_, err = Open(db.GetOptions())
		assert.NotNil(err)
	}
}

func TestShrink(t *testing.T) {
	assert := assert.New(t)
	db, _ := createDB()

	for i := 0; i < 10000; i++ {
		db.Set(fmt.Sprintf("%06d", i), []byte(fmt.Sprintf("v-%06d", i)))
	}

	// shrink
	err := db.Shrink()
	assert.Nil(err)

	db.Close()

	// reopen
	db2, _ := Open(db.GetOptions())
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("%06d", i)
		val, ts, err := db2.Get(key)
		assert.Equal(val, []byte(fmt.Sprintf("v-%06d", i)))
		assert.Equal(ts, int64(0))
		assert.Nil(err)
	}
}

func TestIncr(t *testing.T) {
	assert := assert.New(t)
	db, _ := createDB()

	n, err := db.Incr("key", 1)
	assert.Equal(n, int64(1))
	assert.Nil(err)

	n, err = db.Incr("key", 2)
	assert.Equal(n, int64(3))
	assert.Nil(err)

	// Error
	db.Set("ss", []byte("abcde"))
	n, err = db.Incr("ss", 1)
	assert.Equal(n, int64(0))
	assert.NotNil(err)

	// Shrink
	err = db.Shrink()
	assert.Nil(err)
}

func TestBatchSet(t *testing.T) {
	assert := assert.New(t)
	db, _ := createDB()

	batch := make([]*Batch, 0)
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("%08d", i)
		v := fmt.Sprintf("v-%08d", i)
		batch = append(batch, &Batch{
			Key: k,
			Val: []byte(v),
		})
	}
	db.BatchSet(batch...)

	// get
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("%08d", i)
		val, ts, err := db.Get(k)
		assert.Equal(string(val), fmt.Sprintf("v-%08d", i))
		assert.Equal(ts, int64(0))
		assert.Nil(err)
	}

	db.Shrink()
	db.Close()

	// reopen
	db2, _ := Open(db.GetOptions())
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("%08d", i)
		val, ts, err := db2.Get(k)
		assert.Equal(string(val), fmt.Sprintf("v-%08d", i))
		assert.Equal(ts, int64(0))
		assert.Nil(err)
	}
}
