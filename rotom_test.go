package rotom

import (
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
		if v.Ts < cache.GetUnixNano() {
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
		if v.Ts < cache.GetUnixNano() {
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
		now := cache.GetUnixNano()

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

func FuzzHMap(f *testing.F) {
	db, err := Open(NoPersistentConfig)
	if err != nil {
		panic(err)
	}

	f.Fuzz(func(t *testing.T, key, field string, value []byte) {
		assert := assert.New(t)
		err := db.HSet(key, field, value)
		assert.Nil(err)

		res, err := db.HGet(key, field)
		assert.Equal(res, value)
		assert.Nil(err)
	})
}
