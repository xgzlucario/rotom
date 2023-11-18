package rotom

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	cache "github.com/xgzlucario/GigaCache"
	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/codeman"
)

type vItem struct {
	Val []byte
	Ts  int64
}

var (
	nilBytes   []byte
	nilStrings []string
)

func newDBInstance() (*Engine, *Client, error) {
	cfg := DefaultConfig
	cfg.Path = gofakeit.UUID() + ".db"

	db, err := Open(cfg)
	if err != nil {
		return nil, nil, err
	}

	port := gofakeit.Number(10000, 20000)
	addr := "localhost:" + strconv.Itoa(port)

	// listen
	go db.Listen(addr)
	time.Sleep(time.Millisecond)

	cli, err := NewClient(addr)
	if err != nil {
		return nil, nil, err
	}

	return db, cli, nil
}

func TestDB(t *testing.T) {
	println("===== TestDB =====")
	assert := assert.New(t)

	db, cli, err := newDBInstance()
	assert.Nil(err)

	// Test db operations
	db.Set("test1", []byte("2.5"))
	db.SetEx("test2", []byte("2"), time.Minute)
	db.SetTx("test3", []byte("2"), -1)

	assert.ElementsMatch(db.Keys(), []string{"test1", "test2"})
	db.Scan(func(key string, val []byte, ts int64) bool {
		if key == "test1" {
			assert.Equal(val, []byte("2.5"))
			assert.Equal(ts, int64(0))
		} else if key == "test2" {
			assert.Equal(val, []byte("2"))
		} else {
			panic("wrong key")
		}
		return true
	})

	// Client
	cli.Set("foo", []byte("bar"))
	cli.SetEx("foo1", []byte("bar"), time.Minute)
	cli.HSet("map", "foo", []byte("bar"))

	// Error
	num, err := cli.Len()
	assert.Nil(err)
	assert.Equal(num, uint64(5))

	// Get
	val, err := cli.Get("foo")
	assert.Equal(val, []byte("bar"))
	assert.Nil(err)

	val, err = cli.Get("map")
	assert.Equal(val, nilBytes)
	assert.Equal(err, base.ErrTypeAssert)

	val, err = cli.Get("none")
	assert.Equal(val, nilBytes)
	assert.Equal(err, base.ErrKeyNotFound)

	cli.Set("num", []byte("0"))
	cli.Set("foo2", []byte("abc"))

	// Remove
	sum, err := cli.Remove("foo2")
	assert.Equal(sum, 1)
	assert.Nil(err)

	sum, err = cli.Remove("foo2")
	assert.Equal(sum, 0)
	assert.Nil(err)

	time.Sleep(time.Second)

	// close
	assert.Nil(db.Close())
	assert.Equal(db.Close(), base.ErrDatabaseClosed)

	// Load Error
	os.WriteFile(db.Config.Path, []byte("fake"), 0644)
	_, err = Open(db.Config)
	assert.Equal(err, codeman.ErrParseData)

	// error data type
	db.encode(NewCodec(OpSetTx).Int(100).Str("key").Str("val"))
	db.Close()
	_, err = Open(db.Config)
	assert.Equal(err, codeman.ErrParseData)
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

	db, cli, err := newDBInstance()
	assert.Nil(err)
	defer cli.Close()

	for i := 0; i < 1000; i++ {
		// HSet
		key := "key" + strconv.Itoa(i/100)
		err := cli.HSet("map", key, []byte(key))
		assert.Nil(err)

		// HGet
		res, err := cli.HGet("map", key)
		assert.Nil(err)
		assert.Equal(res, []byte(key))

		// HLen
		num, err := cli.HLen("map")
		assert.Nil(err)
		assert.Equal(num, 1)

		// HKeys
		keys, err := cli.HKeys("map")
		assert.Nil(err)
		assert.ElementsMatch(keys, []string{key})

		// HRemove
		n, err := cli.HRemove("map", key)
		assert.Nil(err)
		assert.Equal(n, 1)
	}

	// Error
	cli.Set("fake", []byte("123"))

	err = cli.HSet("fake", "a", []byte("b"))
	assert.ErrorContains(err, base.ErrWrongType.Error())

	res, err := cli.HLen("fake")
	assert.Equal(res, 0)
	assert.ErrorContains(err, base.ErrWrongType.Error())

	m, err := cli.HKeys("fake")
	assert.Equal(m, nilStrings)
	assert.ErrorContains(err, base.ErrWrongType.Error())

	n, err := cli.HRemove("fake", "foo")
	assert.Equal(n, 0)
	assert.ErrorContains(err, base.ErrWrongType.Error())

	cli.HSet("map", "m1", []byte("m2"))
	{
		res, err := cli.Get("map")
		assert.Nil(res)
		assert.Equal(err, base.ErrTypeAssert)
	}
	{
		res, err := cli.HGet("fake", "none")
		assert.Nil(res)
		assert.ErrorContains(err, base.ErrWrongType.Error())
	}
	{
		res, err := cli.HGet("map", "none")
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

	db, cli, err := newDBInstance()
	assert.Nil(err)

	for i := 0; i < 1000; i++ {
		key := gofakeit.UUID()
		animal := gofakeit.Animal()

		err = cli.RPush(key, animal)
		assert.Nil(err)

		res, err := cli.LPop(key)
		assert.Nil(err)
		assert.Equal(res, animal)

		num, err := cli.LLen(key)
		assert.Nil(err)
		assert.Equal(num, 0)
	}

	for i := 0; i < 1000; i++ {
		key := gofakeit.UUID()
		animal := gofakeit.Animal()

		err = cli.LPush(key, animal)
		assert.Nil(err)

		res, err := cli.RPop(key)
		assert.Nil(err)
		assert.Equal(res, animal)

		num, err := cli.LLen(key)
		assert.Nil(err)
		assert.Equal(num, 0)
	}

	// Error
	cli.HSet("map", "key", []byte("value"))

	err = cli.LPush("map", "1")
	assert.ErrorContains(err, base.ErrWrongType.Error())

	err = cli.RPush("map", "1")
	assert.ErrorContains(err, base.ErrWrongType.Error())

	res, err := cli.LPop("map")
	assert.Equal(res, "")
	assert.ErrorContains(err, base.ErrWrongType.Error())

	res, err = cli.RPop("map")
	assert.Equal(res, "")
	assert.ErrorContains(err, base.ErrWrongType.Error())

	n, err := cli.LLen("map")
	assert.Equal(n, 0)
	assert.ErrorContains(err, base.ErrWrongType.Error())

	cli.RPush("list", "1")
	cli.RPop("list")

	// empty list
	res, err = cli.LPop("list")
	assert.Equal(res, "")
	assert.Equal(err, base.ErrEmptyList)

	res, err = cli.RPop("list")
	assert.Equal(res, "")
	assert.Equal(err, base.ErrEmptyList)

	for i := 0; i < 100; i++ {
		cli.RPush("list", gofakeit.Animal())
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

	db, cli, err := newDBInstance()
	assert.Nil(err)

	// SAdd
	for i := 0; i < 1000; i++ {
		r, err := cli.Ping()
		assert.Nil(err)
		assert.Equal(r, []byte("pong"))

		n, err := cli.SAdd("set"+strconv.Itoa(i/100), strconv.Itoa(i))
		assert.Equal(n, 1)
		assert.Nil(err)
	}
	// SHas
	for i := 500; i < 1500; i++ {
		ok, err := cli.SHas("set"+strconv.Itoa(i/100), strconv.Itoa(i))
		assert.Nil(err)
		assert.Equal(ok, i < 1000)
	}
	// SRemove
	for i := 0; i < 1000; i++ {
		key := "set" + strconv.Itoa(i/100)

		if i%2 == 0 {
			assert.Nil(cli.SRemove(key, strconv.Itoa(i)))
		} else {
			_, err := cli.SPop(key)
			assert.Nil(err)
			// assert.Equal(res, strconv.Itoa(i))
		}

		err = cli.SRemove(key, "none")
		assert.Nil(err)

		// SCard SMembers
		n, err1 := cli.SCard(key)
		m, err2 := cli.SMembers(key)
		assert.Nil(err1)
		assert.Nil(err2)
		assert.Equal(n, len(m))
	}

	// Union
	for i := 0; i < 1000; i++ {
		// Add random data
		for i := 0; i < 20; i++ {
			stri := strconv.Itoa(i)
			cli.SAdd("a"+stri, gofakeit.Animal())
			cli.SAdd("b"+stri, gofakeit.Animal())
		}
		stri := strconv.Itoa(i)

		err = cli.SUnion("union"+stri, "a"+stri, "b"+stri)
		assert.Nil(err)

		err = cli.SInter("inter"+stri, "a"+stri, "b"+stri)
		assert.Nil(err)

		err = cli.SDiff("diff"+stri, "a"+stri, "b"+stri)
		assert.Nil(err)

		// diff + inter = union
		cli.SUnion("res"+stri, "inter"+stri, "diff"+stri)

		m1, err1 := cli.SMembers("union" + stri)
		assert.Nil(err1)
		m2, err2 := cli.SMembers("res" + stri)
		assert.Nil(err2)
		assert.ElementsMatch(m1, m2)
	}

	// Error
	cli.HSet("map", "key", []byte("1"))
	n, err := cli.SAdd("map", "1")
	assert.Equal(n, 0)
	assert.ErrorContains(err, base.ErrWrongType.Error())

	ok, err := cli.SHas("map", "1")
	assert.False(ok)
	assert.ErrorContains(err, base.ErrWrongType.Error())

	res, err := cli.SPop("map")
	assert.Equal(res, "")
	assert.ErrorContains(err, base.ErrWrongType.Error())

	err = cli.SRemove("map", "1")
	assert.ErrorContains(err, base.ErrWrongType.Error())

	n, err = cli.SCard("map")
	assert.Equal(n, 0)
	assert.ErrorContains(err, base.ErrWrongType.Error())

	m, err := cli.SMembers("map")
	assert.Equal(m, nilStrings)
	assert.ErrorContains(err, base.ErrWrongType.Error())

	err = cli.SUnion("map", "map")
	assert.ErrorContains(err, base.ErrWrongType.Error())

	err = cli.SDiff("map", "map")
	assert.ErrorContains(err, base.ErrWrongType.Error())

	err = cli.SInter("map", "map")
	assert.ErrorContains(err, base.ErrWrongType.Error())

	db.Shrink()
	db.Close()

	// Load
	_, err = Open(db.Config)
	assert.Nil(err)
}

func TestBitmap(t *testing.T) {
	println("===== TestBitmap =====")
	assert := assert.New(t)

	db, cli, err := newDBInstance()
	assert.Nil(err)

	for i := 0; i < 1000; i++ {
		key := strconv.Itoa(i / 100)

		assert.Nil(cli.BitSet(key, uint32(i), true))
		assert.Nil(cli.BitSet(key, uint32(i), false))
		assert.Nil(cli.BitSet(key, uint32(i), true))

		ok, err := cli.BitTest(key, uint32(i))
		assert.True(ok)
		assert.Nil(err)

		// TODO
		cli.BitFlip(key, uint32(i))

		// Error
		cli.Set("none", []byte("1"))
		err = cli.BitSet("none", uint32(i), true)
		assert.ErrorContains(err, base.ErrWrongType.Error())

		ok, err = cli.BitTest("none", uint32(i))
		assert.False(ok)
		assert.ErrorContains(err, base.ErrWrongType.Error())

		err = cli.BitFlip("none", uint32(i))
		assert.ErrorContains(err, base.ErrWrongType.Error())

		m, err := cli.BitArray("none")
		assert.Nil(m)
		assert.ErrorContains(err, base.ErrWrongType.Error())

		n, err := cli.BitCount("none")
		assert.Equal(n, uint64(0))
		assert.ErrorContains(err, base.ErrWrongType.Error())

		err = cli.BitAnd("none", "none")
		assert.ErrorContains(err, base.ErrWrongType.Error())

		err = cli.BitOr("none", "none")
		assert.ErrorContains(err, base.ErrWrongType.Error())

		err = cli.BitXor("none", "none")
		assert.ErrorContains(err, base.ErrWrongType.Error())
	}

	for i := 0; i < 1000; i++ {
		// Add random data
		for i := 0; i < 20; i++ {
			stri := strconv.Itoa(i)
			cli.BitSet("a"+stri, rand.Uint32(), true)
			cli.BitSet("b"+stri, rand.Uint32(), true)
		}
		stri := strconv.Itoa(i)

		err = cli.BitOr("or"+stri, "a"+stri, "b"+stri)
		assert.Nil(err)

		err = cli.BitAnd("and"+stri, "a"+stri, "b"+stri)
		assert.Nil(err)

		err = cli.BitXor("xor"+stri, "a"+stri, "b"+stri)
		assert.Nil(err)

		// xor + and = or
		cli.BitOr("res"+stri, "and"+stri, "xor"+stri)

		m1, err1 := cli.BitArray("or" + stri)
		assert.Nil(err1)
		n1, errn1 := cli.BitCount("or" + stri)
		assert.Nil(errn1)
		assert.Equal(uint64(len(m1)), n1)

		m2, err2 := cli.BitArray("res" + stri)
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

	db, cli, err := newDBInstance()
	assert.Nil(err)

	// ZAdd
	for i := 0; i < 1000; i++ {
		err := cli.ZAdd("zset", fmt.Sprintf("key-%d", i), float64(i), nil)
		assert.Nil(err)
	}

	// ZIncr
	for i := 0; i < 1000; i++ {
		num, err := cli.ZIncr("zset", fmt.Sprintf("key-%d", i), 3)
		assert.Nil(err)
		assert.Equal(num, float64(i+3))
	}

	// ZRemove
	for i := 0; i < 1000; i++ {
		err := cli.ZRemove("zset", fmt.Sprintf("key-%d", i))
		assert.Nil(err)
	}

	// Test error
	cli.SAdd("set", "1")

	err = cli.ZAdd("set", "key", 1, nil)
	assert.ErrorContains(err, base.ErrWrongType.Error())

	_, err = cli.ZIncr("set", "key", 1)
	assert.ErrorContains(err, base.ErrWrongType.Error())

	err = cli.ZRemove("set", "key")
	assert.ErrorContains(err, base.ErrWrongType.Error())

	// load
	db.Shrink()
	db.Close()
	_, err = Open(db.Config)
	assert.Nil(err)
}

func TestUtils(t *testing.T) {
	println("===== TestUtils =====")
	assert := assert.New(t)

	db, cli, err := newDBInstance()
	assert.Nil(err)
	assert.NotNil(cli)

	cd, err := NewCodec(OpSetTx).Any("string")
	assert.Nil(cd)
	assert.NotNil(err)

	decoder := codeman.NewDecoder(nil)
	_, err = decoder.Parses(2)
	assert.Equal(err, codeman.ErrDecoderIsDone)

	// fake
	decoder = codeman.NewDecoder([]byte{1, 2, 3, 4})
	_, err = decoder.Parses(2)
	assert.Equal(err, codeman.ErrParseData)

	decoder = codeman.NewDecoder([]byte{byte(OpSetTx), 10, 255})
	_, err = decoder.Parses(2)
	assert.Equal(err, codeman.ErrParseData)

	// handle
	w, err := db.handleEvent([]byte{1, 2, 3, 4, 5})
	assert.Nil(w)
	assert.Equal(err, codeman.ErrParseData)

	cli.b = make([]byte, 1)
	_, err = cli.do(NewCodec(OpSAdd).Str("test"))
	assert.Equal(err, codeman.ErrParseData)
}
