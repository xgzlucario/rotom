package rotom

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	cache "github.com/xgzlucario/GigaCache"
	"github.com/xgzlucario/rotom/base"
	"golang.org/x/exp/maps"
)

type vItem struct {
	Val []byte
	Ts  int64
}

var (
	nilBytes   []byte
	nilStrings []string
)

func createDB() (*Engine, error) {
	cfg := DefaultConfig
	cfg.Path = fmt.Sprintf("%s-%d.db", gofakeit.UUID(), time.Now().UnixNano())
	return Open(cfg)
}

func TestDB(t *testing.T) {
	println("===== TestDB =====")
	assert := assert.New(t)

	db, err := createDB()
	assert.Nil(err)

	m := make(map[string][]byte)

	// Test db operations
	for i := 0; i < 1000; i++ {
		key := strconv.Itoa(i)
		db.Set("set-"+key, []byte(strconv.Itoa(i)))
		db.SetEx("setex-"+key, []byte(strconv.Itoa(i)), time.Minute)
		db.SetTx("settx-"+key, []byte(strconv.Itoa(i)), -1)

		m["set-"+key] = []byte(strconv.Itoa(i))
		m["setex-"+key] = []byte(strconv.Itoa(i))
		// m["settx-"+key] = []byte(strconv.Itoa(i))
	}

	assert.ElementsMatch(db.Keys(), maps.Keys(m))

	db.Scan(func(key string, val []byte, ts int64) bool {
		prefix := strings.Split(key, "-")[0]
		switch prefix {
		case "set":
			assert.Equal(val, m[key])
		case "setex":
			assert.Equal(ts > cache.GetClock(), true)
			assert.Equal(val, m[key])
		case "settx":
			assert.Equal(val, m[key])
		default:
			panic("wrong key")
		}
		return true
	})

	// Len
	num := db.Len()
	assert.Equal(int(num), len(m))

	// Get
	for k, v := range m {
		val, _, err := db.Get(k)

		prefix := strings.Split(k, "-")[0]
		switch prefix {
		case "set":
			assert.Equal(val, v)
		case "setex":
			assert.Equal(val, v)
		case "settx":
			assert.Equal(nil, v)
			assert.Equal(err, base.ErrKeyNotFound)
		default:
			panic("wrong key")
		}
	}

	// Error
	val, _, err := db.Get("map")
	assert.Equal(val, nilBytes)
	assert.Equal(err, base.ErrKeyNotFound)

	val, _, err = db.Get("none")
	assert.Equal(val, nilBytes)
	assert.Equal(err, base.ErrKeyNotFound)

	// Remove
	sum := db.Remove("set-1", "set-2", "set-3")
	assert.Equal(sum, 3)

	sum = db.Remove("set-1", "set-2", "set-3")
	assert.Equal(sum, 0)

	// close
	assert.Nil(db.Close())
	assert.Equal(db.Close(), base.ErrDatabaseClosed)

	// Load Success
	_, err = Open(db.Config)
	assert.Nil(err)

	// Load Error
	os.WriteFile(db.Config.Path, []byte("fake"), 0644)
	_, err = Open(db.Config)
	assert.NotNil(err, base.ErrCheckSum)

	// error data type
	db.encode(NewCodec(OpSetTx).Int(100).Str("key").Str("val"))
	db.Close()
	_, err = Open(db.Config)
	assert.NotNil(err, base.ErrCheckSum)
}

func TestSetTTL(t *testing.T) {
	println("===== TestSetTTL =====")
	assert := assert.New(t)

	cfg := DefaultConfig
	cfg.Path = gofakeit.UUID() + ".db"

	db, err := Open(cfg)
	assert.Nil(err)

	// generate test data
	num := 10000
	kvdata := make(map[string]vItem, num)
	now := time.Now()

	for i := 0; i < num; i++ {
		key := gofakeit.UUID()
		val := []byte(gofakeit.Username())
		ts := now.Add(time.Second * time.Duration(gofakeit.Number(0, 100))).UnixNano()

		kvdata[key] = vItem{val, ts}

		// set
		db.SetTx(key, val, ts)
	}

	// get
	for k, v := range kvdata {
		// expired
		if v.Ts < cache.GetClock() {
			val, ts, err := db.Get(k)
			assert.Equal(val, nilBytes)
			assert.Equal(ts, int64(0))
			assert.NotNil(err)

		} else {
			val, ts, err := db.Get(k)
			assert.Equal(val, v.Val)
			assert.Equal(ts, v.Ts)
			assert.Nil(err)
		}
	}

	err = db.Close()
	assert.Nil(err)

	// load
	db, err = Open(cfg)
	assert.Nil(err)

	// get again
	for k, v := range kvdata {
		v.Ts /= timeCarry
		v.Ts *= timeCarry

		// expired
		if v.Ts < cache.GetClock() {
			_, _, err := db.Get(k)
			assert.NotNil(err)

		} else {
			val, ts, err := db.Get(k)
			assert.Equal(val, v.Val)
			assert.Equal(ts, v.Ts)
			assert.Nil(err)
		}
	}
}

func TestHmap(t *testing.T) {
	println("===== TestHmap =====")
	assert := assert.New(t)

	db, err := createDB()
	assert.Nil(err)
	defer db.Close()

	for i := 0; i < 1000; i++ {
		// HSet
		key := "key" + strconv.Itoa(i/100)
		err := db.HSet("map", key, []byte(key))
		assert.Nil(err)

		// HGet
		res, err := db.HGet("map", key)
		assert.Nil(err)
		assert.Equal(res, []byte(key))

		// HLen
		num, err := db.HLen("map")
		assert.Nil(err)
		assert.Equal(num, 1)

		// HKeys
		keys, err := db.HKeys("map")
		assert.Nil(err)
		assert.ElementsMatch(keys, []string{key})

		// HRemove
		n, err := db.HRemove("map", key)
		assert.Nil(err)
		assert.Equal(n, 1)
	}

	// Error
	db.Set("fake", []byte("123"))

	err = db.HSet("fake", "a", []byte("b"))
	assert.ErrorContains(err, base.ErrWrongType.Error())

	res, err := db.HLen("fake")
	assert.Equal(res, 0)
	assert.ErrorContains(err, base.ErrWrongType.Error())

	m, err := db.HKeys("fake")
	assert.Equal(m, nilStrings)
	assert.ErrorContains(err, base.ErrWrongType.Error())

	n, err := db.HRemove("fake", "foo")
	assert.Equal(n, 0)
	assert.ErrorContains(err, base.ErrWrongType.Error())

	db.HSet("map", "m1", []byte("m2"))
	{
		res, _, err := db.Get("map")
		assert.Nil(res)
		assert.Equal(err, base.ErrTypeAssert)
	}
	{
		res, err := db.HGet("fake", "none")
		assert.Nil(res)
		assert.ErrorContains(err, base.ErrWrongType.Error())
	}
	{
		res, err := db.HGet("map", "none")
		assert.Nil(res)
		assert.Equal(err, base.ErrFieldNotFound)
	}

	db.Shrink()
	db.Close()

	// Load
	_, err = Open(db.Config)
	assert.Nil(err)
}

func TestList(t *testing.T) {
	println("===== TestList =====")
	assert := assert.New(t)

	db, err := createDB()
	assert.Nil(err)

	for i := 0; i < 1000; i++ {
		key := gofakeit.UUID()
		animal := gofakeit.Animal()

		err = db.LRPush(key, animal)
		assert.Nil(err)

		res, err := db.LLPop(key)
		assert.Nil(err)
		assert.Equal(res, animal)

		num, err := db.LLen(key)
		assert.Nil(err)
		assert.Equal(num, 0)
	}

	for i := 0; i < 1000; i++ {
		key := gofakeit.UUID()
		animal := gofakeit.Animal()

		err = db.LPush(key, animal)
		assert.Nil(err)

		// Index
		res, err := db.LIndex(key, 0)
		assert.Nil(err)
		assert.Equal(res, animal)

		res, err = db.LRPop(key)
		assert.Nil(err)
		assert.Equal(res, animal)

		num, err := db.LLen(key)
		assert.Nil(err)
		assert.Equal(num, 0)
	}

	// Error
	db.HSet("map", "key", []byte("value"))

	err = db.LPush("map", "1")
	assert.ErrorContains(err, base.ErrWrongType.Error())

	err = db.LRPush("map", "1")
	assert.ErrorContains(err, base.ErrWrongType.Error())

	res, err := db.LLPop("map")
	assert.Equal(res, "")
	assert.ErrorContains(err, base.ErrWrongType.Error())

	res, err = db.LRPop("map")
	assert.Equal(res, "")
	assert.ErrorContains(err, base.ErrWrongType.Error())

	s, err := db.LIndex("map", 1)
	assert.Equal(s, "")
	assert.ErrorContains(err, base.ErrWrongType.Error())

	n, err := db.LLen("map")
	assert.Equal(n, 0)
	assert.ErrorContains(err, base.ErrWrongType.Error())

	db.LRPush("list", "1")
	db.LRPop("list")

	// empty list
	res, err = db.LLPop("list")
	assert.Equal(res, "")
	assert.Equal(err, base.ErrEmptyList)

	res, err = db.LRPop("list")
	assert.Equal(res, "")
	assert.Equal(err, base.ErrEmptyList)

	res, err = db.LIndex("list", 9)
	assert.Equal(res, "")
	assert.Equal(err, base.ErrIndexOutOfRange)

	for i := 0; i < 100; i++ {
		db.LRPush("list", gofakeit.Animal())
	}

	db.Shrink()
	db.Close()

	// Load
	_, err = Open(db.Config)
	assert.Nil(err)
}

func TestSet(t *testing.T) {
	println("===== TestSet =====")
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
			db.SAdd("a"+stri, gofakeit.Animal())
			db.SAdd("b"+stri, gofakeit.Animal())
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
	db.HSet("map", "key", []byte("1"))
	n, err := db.SAdd("map", "1")
	assert.Equal(n, 0)
	assert.ErrorContains(err, base.ErrWrongType.Error())

	ok, err := db.SHas("map", "1")
	assert.False(ok)
	assert.ErrorContains(err, base.ErrWrongType.Error())

	err = db.SRemove("map", "1")
	assert.ErrorContains(err, base.ErrWrongType.Error())

	n, err = db.SCard("map")
	assert.Equal(n, 0)
	assert.ErrorContains(err, base.ErrWrongType.Error())

	m, err := db.SMembers("map")
	assert.Equal(m, nilStrings)
	assert.ErrorContains(err, base.ErrWrongType.Error())

	err = db.SUnion("map", "map")
	assert.ErrorContains(err, base.ErrWrongType.Error())

	err = db.SDiff("map", "map")
	assert.ErrorContains(err, base.ErrWrongType.Error())

	err = db.SInter("map", "map")
	assert.ErrorContains(err, base.ErrWrongType.Error())

	db.Shrink()
	db.Close()

	// Load
	// TODO: fix
	_, err = Open(db.Config)
	assert.Nil(err)
}

func TestBitmap(t *testing.T) {
	println("===== TestBitmap =====")
	assert := assert.New(t)

	db, err := createDB()
	assert.Nil(err)

	for i := 0; i < 1000; i++ {
		key := strconv.Itoa(i / 100)

		assert.Nil(db.BitSet(key, uint32(i), true))
		assert.Nil(db.BitSet(key, uint32(i), false))
		assert.Nil(db.BitSet(key, uint32(i), true))

		ok, err := db.BitTest(key, uint32(i))
		assert.True(ok)
		assert.Nil(err)

		// TODO
		db.BitFlip(key, uint32(i))

		// Error
		db.Set("none", []byte("1"))
		err = db.BitSet("none", uint32(i), true)
		assert.ErrorContains(err, base.ErrWrongType.Error())

		ok, err = db.BitTest("none", uint32(i))
		assert.False(ok)
		assert.ErrorContains(err, base.ErrWrongType.Error())

		err = db.BitFlip("none", uint32(i))
		assert.ErrorContains(err, base.ErrWrongType.Error())

		m, err := db.BitArray("none")
		assert.Nil(m)
		assert.ErrorContains(err, base.ErrWrongType.Error())

		n, err := db.BitCount("none")
		assert.Equal(n, uint64(0))
		assert.ErrorContains(err, base.ErrWrongType.Error())

		err = db.BitAnd("none", "none")
		assert.ErrorContains(err, base.ErrWrongType.Error())

		err = db.BitOr("none", "none")
		assert.ErrorContains(err, base.ErrWrongType.Error())

		err = db.BitXor("none", "none")
		assert.ErrorContains(err, base.ErrWrongType.Error())
	}

	for i := 0; i < 1000; i++ {
		// Add random data
		for i := 0; i < 20; i++ {
			stri := strconv.Itoa(i)
			db.BitSet("a"+stri, rand.Uint32(), true)
			db.BitSet("b"+stri, rand.Uint32(), true)
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

	db.Shrink()
	db.Close()

	// Load
	_, err = Open(db.Config)
	assert.Nil(err)
}

func TestZSet(t *testing.T) {
	println("===== TestZSet =====")
	assert := assert.New(t)

	db, err := createDB()
	assert.Nil(err)

	// ZAdd
	for i := 0; i < 1000; i++ {
		err := db.ZAdd("zset", fmt.Sprintf("key-%d", i), float64(i), nil)
		assert.Nil(err)
	}

	// ZIncr
	for i := 0; i < 1000; i++ {
		num, err := db.ZIncr("zset", fmt.Sprintf("key-%d", i), 3)
		assert.Nil(err)
		assert.Equal(num, float64(i+3))
	}

	// ZRemove
	for i := 0; i < 1000; i++ {
		err := db.ZRemove("zset", fmt.Sprintf("key-%d", i))
		assert.Nil(err)
	}

	// Test error
	db.SAdd("set", "1")

	err = db.ZAdd("set", "key", 1, nil)
	assert.ErrorContains(err, base.ErrWrongType.Error())

	_, err = db.ZIncr("set", "key", 1)
	assert.ErrorContains(err, base.ErrWrongType.Error())

	err = db.ZRemove("set", "key")
	assert.ErrorContains(err, base.ErrWrongType.Error())

	// load
	db.Shrink()
	db.Close()

	_, err = Open(db.Config)
	assert.Nil(err)
}

func TestUtils(t *testing.T) {
	assert := assert.New(t)

	assert.Panics(func() {
		base.NewTicker(context.TODO(), -1, func() {})
	})

	ctx, cancel := context.WithCancel(context.Background())
	ticker := base.NewTicker(ctx, time.Second, func() {})
	ticker.Reset(time.Second)

	cancel()
	err := ticker.Do()
	assert.Equal(err, base.ErrTickerClosed)
}
