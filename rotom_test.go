package rotom

import (
	"os"
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
	nilBytes []byte
)

func TestDB(t *testing.T) {
	assert := assert.New(t)

	cfg := DefaultConfig
	cfg.Path = gofakeit.UUID() + ".db"

	db, err := Open(cfg)
	assert.Nil(err)
	assert.NotNil(db)

	// Set
	db.Set("foo", []byte("bar"))
	db.Set("num", []byte("1"))
	db.HSet("hm", "foo", []byte("bar"))

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

	// close
	assert.Nil(db.Close())
	assert.NotNil(db.Close())

	// load error
	os.WriteFile(cfg.Path, []byte("fake data"), 0644)
	db, err = Open(cfg)
	assert.NotNil(err)
	assert.Nil(db)
}

// Test cache set operation
func TestCacheSet(t *testing.T) {
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
		v.Ts /= (1000 * 1000 * 1000)
		v.Ts *= (1000 * 1000 * 1000)

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

// TestBitmap
func TestBitmap(t *testing.T) {
	assert := assert.New(t)

	cfg := DefaultConfig
	cfg.Path = gofakeit.UUID() + ".db"

	db, err := Open(cfg)
	assert.Nil(err)

	// valid
	const num = 10000
	vmap := make(map[uint32]struct{}, num)

	for i := 0; i < num; i++ {
		n := gofakeit.Uint32()
		vmap[n] = struct{}{}
		db.BitSet("bm", n, true)
	}

	// len
	c, err := db.BitCount("bm")
	assert.Nil(err)
	assert.Equal(c, uint64(len(vmap)))

	test := func() {
		for i := uint32(0); i < num; i++ {
			_, ok1 := vmap[i]
			ok2, err := db.BitTest("bm", i)

			assert.Nil(err)
			assert.Equal(ok1, ok2)
		}
	}

	test()

	err = db.Close()
	assert.Nil(err)

	// load
	db, err = Open(cfg)
	assert.Nil(err)

	test()
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

func TestHMap(t *testing.T) {
	assert := assert.New(t)

	db, err := Open(NoPersistentConfig)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 10000; i++ {
		// gen random data
		key := gofakeit.UUID()
		field := gofakeit.UUID()
		value := []byte(gofakeit.Username())
		op := gofakeit.Number(0, 100)

		// test
		err := db.HSet(key, field, value)
		assert.Nil(err)

		res, err := db.HGet(key, field)
		assert.Equal(res, value)
		assert.Nil(err)

		if op%3 == 0 {
			err = db.HRemove(key, field)
			assert.Nil(err)

			res, err = db.HGet(key, field)
			assert.Equal(res, nilBytes)
			assert.Equal(err, base.ErrFieldNotFound)
		}

		if op%5 == 0 {
			keys, err1 := db.HKeys(key)
			length, err2 := db.HLen(key)

			assert.Equal(err1, nil)
			assert.Equal(err2, nil)

			assert.Equal(len(keys), int(length))
		}
	}

	// err test
	db.Set("str", []byte(""))
	{
		// get
		res, err := db.HGet("str", "foo")
		assert.Equal(res, nilBytes)
		assert.Equal(err, base.ErrWrongType)
	}
	{
		// len
		res, err := db.HLen("str")
		assert.Equal(res, 0)
		assert.Equal(err, base.ErrWrongType)
	}
	{
		// set
		err := db.HSet("str", "foo", []byte("bar"))
		assert.Equal(err, base.ErrWrongType)
	}
	{
		// remove
		err := db.HRemove("str", "foo")
		assert.Equal(err, base.ErrWrongType)
	}
	{
		// keys
		res, err := db.HKeys("str")
		var nilSlice []string
		assert.Equal(res, nilSlice)
		assert.Equal(err, base.ErrWrongType)
	}
}
