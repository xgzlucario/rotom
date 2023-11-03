package rotom

import (
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	cache "github.com/xgzlucario/GigaCache"
	"github.com/xgzlucario/rotom/base"
)

type vItem struct {
	Val []byte
	Ts  int64
}

var (
	nilBytes   []byte
	nilStrings []string
)

func TestDB(t *testing.T) {
	assert := assert.New(t)

	cfg := &Config{
		Path:             gofakeit.UUID() + ".db",
		ShardCount:       1024,
		SyncPolicy:       base.EveryInterval,
		SyncInterval:     time.Second,
		ShrinkInterval:   time.Second * 3,
		RunSkipLoadError: true,
		Logger:           slog.Default(),
	}
	db, err := Open(cfg)
	assert.Nil(err)
	assert.NotNil(db)

	// Set
	db.Set("foo", []byte("bar"))
	db.Set("num", []byte("1"))
	db.SetEx("foo1", []byte("bar"), time.Second)
	db.HSet("hm", "foo", []byte("bar"))

	db.Scan(func(s string, a any, i int64) bool {
		if s == "foo" || s == "num" || s == "hm" || s == "foo1" {
			return true
		}
		return false
	})

	// Get
	val, ts, err := db.GetBytes("hm")
	assert.Equal(val, nilBytes)
	assert.Equal(ts, int64(0))
	assert.Equal(err, base.ErrTypeAssert)

	val, ts, err = db.GetBytes("none")
	assert.Equal(val, nilBytes)
	assert.Equal(ts, int64(0))
	assert.Equal(err, base.ErrKeyNotFound)

	// Incr
	res, err := db.Incr("hm", 3.5)
	assert.Equal(res, float64(0))
	assert.Equal(err, base.ErrTypeAssert)

	res, err = db.Incr("foo", 3.5)
	assert.Equal(res, float64(0))
	assert.NotNil(err)

	res, err = db.Incr("num", 3.5)
	assert.Equal(res, 4.5)
	assert.Nil(err)

	// Keys
	assert.ElementsMatch(db.Keys(), []string{"foo", "num", "hm", "foo1"})

	// Rename
	ok := db.Rename("num", "num-new")
	assert.True(ok)
	res, err = db.Incr("num-new", 0.5)
	assert.Equal(res, float64(5))
	assert.Nil(err)

	// Remove
	assert.True(db.Remove("num-new"))
	assert.False(db.Remove("num-new"))

	db.printRuntimeStats()
	time.Sleep(time.Second * 5)

	// close
	assert.Nil(db.Close())
	assert.Equal(db.Close(), base.ErrDatabaseClosed)

	// load error
	os.WriteFile(cfg.Path, []byte("fake data"), 0644)
	db, err = Open(cfg)
	assert.NotNil(err)
	assert.Nil(db)
}

func TestCSet(t *testing.T) {
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
			val, ts, ok := db.Get(k)
			assert.Equal(val, nil)
			assert.Equal(ts, int64(0))
			assert.False(ok)

		} else {
			val, ts, err := db.GetBytes(k)
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
		// timeCarry convert
		v.Ts /= (1000 * 1000)
		v.Ts *= (1000 * 1000)

		// expired
		if v.Ts < cache.GetClock() {
			_, _, ok := db.Get(k)
			assert.False(ok)

		} else {
			val, ts, err := db.GetBytes(k)
			assert.Equal(val, v.Val)
			assert.Equal(ts, v.Ts)
			assert.Equal(err, nil)
		}
	}
}

func FuzzSet(f *testing.F) {
	db, err := Open(NoPersistentConfig)
	if err != nil {
		panic(err)
	}

	f.Fuzz(func(t *testing.T, key string, val []byte, ts int64) {
		assert := assert.New(t)
		db.SetTx(key, val, ts)
		now := cache.GetClock()

		v, ttl, err := db.GetBytes(key)

		// no ttl
		if ts == 0 {
			assert.Equal(v, val)
			assert.Equal(ttl, int64(0))
			assert.Equal(err, nil)

			// expired
		} else if ts < now {
			assert.Equal(v, nil)
			assert.Equal(ttl, int64(0))
			assert.Equal(err, base.ErrKeyNotFound)

			// not expired
		} else if ts > now {
			assert.Equal(v, val)
			assert.Equal(ttl, ts)
			assert.Equal(err, nil)
		}
	})
}

func TestHmap(t *testing.T) {
	assert := assert.New(t)

	cfg := DefaultConfig
	cfg.Path = gofakeit.UUID() + ".db"

	db, err := Open(cfg)
	assert.Nil(err)

	port := gofakeit.Number(10000, 20000)
	addr := "localhost:" + strconv.Itoa(port)

	// listen
	go db.Listen(addr)
	time.Sleep(time.Second / 20)

	cli, err := NewClient(addr)
	assert.Nil(err)
	defer cli.Close()

	for i := 0; i < 10000; i++ {
		// Set
		key := fmt.Sprintf("key-%d", i)
		err := cli.Set(key, []byte(key))
		assert.Nil(err)

		// Get
		res, err := cli.Get(key)
		assert.Nil(err)
		assert.Equal(res, []byte(key))

		// SetEx
		key = fmt.Sprintf("key-ex-%d", i)
		err = cli.SetEx(key, []byte(key), time.Minute)
		assert.Nil(err)

		// Rename
		newKey := fmt.Sprintf("key-new-%d", i)
		ok, err := cli.Rename(key, newKey)
		assert.Nil(err)
		assert.True(ok)

		// Remove
		ok, err = cli.Remove(newKey)
		assert.Nil(err)
		assert.True(ok)

		// Len
		num, err := cli.Len()
		assert.Nil(err)
		assert.Equal(num, uint64(i+1))
	}

	for i := 0; i < 10000; i++ {
		// HSet
		key := fmt.Sprintf("key-%d", i)
		err := cli.HSet("exmap", key, []byte(key))
		assert.Nil(err)

		// HGet
		res, err := cli.HGet("exmap", key)
		assert.Nil(err)
		assert.Equal(res, []byte(key))

		// HLen
		num, err := cli.HLen("exmap")
		assert.Nil(err)
		assert.Equal(num, 1)

		// HKeys
		keys, err := cli.HKeys("exmap")
		assert.Nil(err)
		assert.ElementsMatch(keys, []string{key})

		// HRemove
		ok, err := cli.HRemove("exmap", key)
		assert.Nil(err)
		assert.True(ok)
	}

	// Error
	cli.Set("fake", []byte("123"))

	res, err := cli.HLen("fake")
	assert.Equal(res, 0)
	assert.ErrorContains(err, base.ErrWrongType.Error())
	{
		res, err := cli.HKeys("fake")
		var nilSlice []string
		assert.Equal(res, nilSlice)
		assert.ErrorContains(err, base.ErrWrongType.Error())
	}
	{
		res, err := cli.HRemove("fake", "foo")
		assert.False(res)
		assert.ErrorContains(err, base.ErrWrongType.Error())
	}

	cli.HSet("fakemap", "m1", []byte("m2"))
	{
		res, err := cli.Get("fakemap")
		assert.Nil(res)
		assert.Equal(err, base.ErrTypeAssert)
	}
	{
		res, err := cli.HGet("fake", "none")
		assert.Nil(res)
		assert.ErrorContains(err, base.ErrWrongType.Error())
	}
	{
		res, err := cli.HGet("fakemap", "none")
		assert.Nil(res)
		assert.Equal(err, base.ErrFieldNotFound)
	}
}

func TestSet(t *testing.T) {
	assert := assert.New(t)

	cfg := DefaultConfig
	cfg.Path = gofakeit.UUID() + ".db"

	db, err := Open(cfg)
	assert.Nil(err)

	port := gofakeit.Number(10000, 20000)
	addr := "localhost:" + strconv.Itoa(port)

	// listen
	go db.Listen(addr)
	time.Sleep(time.Second / 20)

	cli, err := NewClient(addr)
	assert.Nil(err)

	// SAdd
	for i := 0; i < 1000; i++ {
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
		err := cli.SRemove(key, strconv.Itoa(i))
		assert.Nil(err)

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
			cli.SAdd("a"+strconv.Itoa(i), strconv.Itoa(rand.Intn(10)))
			cli.SAdd("b"+strconv.Itoa(i), strconv.Itoa(rand.Intn(10)))
		}

		err = cli.SUnion("union"+strconv.Itoa(i), "a"+strconv.Itoa(i), "b"+strconv.Itoa(i))
		assert.Nil(err)

		err = cli.SInter("inter"+strconv.Itoa(i), "a"+strconv.Itoa(i), "b"+strconv.Itoa(i))
		assert.Nil(err)

		err = cli.SDiff("diff"+strconv.Itoa(i), "a"+strconv.Itoa(i), "b"+strconv.Itoa(i))
		assert.Nil(err)

		// diff + inter = union
		cli.SUnion("res"+strconv.Itoa(i), "inter"+strconv.Itoa(i), "diff"+strconv.Itoa(i))

		m1, err1 := cli.SMembers("union" + strconv.Itoa(i))
		assert.Nil(err1)
		m2, err2 := cli.SMembers("res" + strconv.Itoa(i))
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

	n, err = cli.SCard("map")
	assert.Equal(n, 0)
	assert.ErrorContains(err, base.ErrWrongType.Error())

	m, err := cli.SMembers("map")
	assert.Equal(m, nilStrings)
	assert.ErrorContains(err, base.ErrWrongType.Error())

	db.Shrink()
	db.Close()

	// Load
	// _, err = Open(cfg)
	// assert.Nil(err)
}

func TestBitmap(t *testing.T) {
	assert := assert.New(t)

	cfg := DefaultConfig
	cfg.Path = gofakeit.UUID() + ".db"

	db, err := Open(cfg)
	assert.Nil(err)

	port := gofakeit.Number(10000, 20000)
	addr := "localhost:" + strconv.Itoa(port)

	// listen
	go db.Listen(addr)
	time.Sleep(time.Second / 20)

	cli, err := NewClient(addr)
	assert.Nil(err)

	for i := 0; i < 1000; i++ {
		key := strconv.Itoa(i / 100)

		assert.Nil(cli.BitSet(key, uint32(i), true))

		ok, err := cli.BitTest(key, uint32(i))
		assert.True(ok)
		assert.Nil(err)
	}

	db.Shrink()
	db.Close()

	// Load
	_, err = Open(cfg)
	assert.Nil(err)
}

func TestZSet(t *testing.T) {
	assert := assert.New(t)

	db, err := Open(NoPersistentConfig)
	assert.Nil(err)

	// ZAdd
	for i := 0; i < 10000; i++ {
		err := db.ZAdd("zset", fmt.Sprintf("key-%d", i), float64(i), nil)
		assert.Nil(err)
	}

	// ZIncr
	for i := 0; i < 10000; i++ {
		num, err := db.ZIncr("zset", fmt.Sprintf("key-%d", i), 3)
		assert.Nil(err)
		assert.Equal(num, float64(i+3))
	}

	// ZRemove
	for i := 0; i < 10000; i++ {
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
	db.Close()
}
