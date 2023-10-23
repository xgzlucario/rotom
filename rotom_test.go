package rotom

import (
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	"golang.org/x/exp/rand"

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
	source   = rand.NewSource(uint64(time.Now().UnixNano()))
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

	// Keys
	assert.ElementsMatch(db.Keys(), []string{"foo", "num", "hm"})

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
	go db.Listen("localhost:7676")
	time.Sleep(time.Second * 5)

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

func setEqualBitmap(assert *assert.Assertions, db *Engine, skey, bkey string) {
	// Card
	n1, err1 := db.SCard(skey)
	n2, err2 := db.BitCount(bkey)
	assert.Equal(n1, int(n2))
	assert.Nil(err1)
	assert.Nil(err2)

	// Members
	s1, err1 := db.SMembers(skey)
	s2, err2 := db.BitArray(bkey)
	// s2 -> []string
	strslices := make([]string, 0, len(s2))
	for _, v := range s2 {
		strslices = append(strslices, strconv.Itoa(int(v)))
	}
	assert.ElementsMatch(s1, strslices)
	assert.Nil(err1)
	assert.Nil(err2)
}

func randUint16() uint32 {
	return uint32(source.Uint64() >> 48)
}

func TestSetAndBitmap(t *testing.T) {
	assert := assert.New(t)

	cfg := DefaultConfig
	cfg.Path = gofakeit.UUID() + ".db"
	db, err := Open(cfg)
	assert.Nil(err)

	type keyPair struct {
		skey, bkey string
	}
	keyPairMap := make(map[keyPair]struct{}, 30000)

	for i := 0; i < 30000; i++ {
		rand := gofakeit.Username()
		k1, k2 := "S"+rand, "B"+rand
		val := randUint16()

		// Add
		err := db.SAdd(k1, strconv.Itoa(int(val)))
		assert.Nil(err)
		_, err = db.BitSet(k2, val, true)
		assert.Nil(err)

		keyPairMap[keyPair{k1, k2}] = struct{}{}

		// Remove
		if gofakeit.Number(0, 10) > 5 {
			val := randUint16()
			// Has
			ok1, err1 := db.SHas(k1, strconv.Itoa(int(val)))
			ok2, err2 := db.BitTest(k2, val)
			assert.Equal(ok1, ok2)
			assert.Nil(err1)
			assert.Nil(err2)
			{
				// Remove
				ok3, err3 := db.SRemove(k1, strconv.Itoa(int(val)))
				ok4, err4 := db.BitSet(k2, val, false)
				assert.Equal(ok1, ok3)
				assert.Equal(ok3, ok4)
				assert.Nil(err3)
				assert.Nil(err4)
			}
		}

		setEqualBitmap(assert, db, k1, k2)

		// Test Union
		if gofakeit.Number(0, 10) > 8 {
			var kp1, kp2, dstp keyPair
			for k := range keyPairMap {
				kp1 = k
				break
			}
			for k := range keyPairMap {
				kp2 = k
				break
			}
			for k := range keyPairMap {
				dstp = k
				break
			}

			// Inplace
			switch gofakeit.Number(1, 10) {
			case 1:
				kp1 = kp2
			case 2:
				kp2 = dstp
			case 3:
				kp1 = dstp
			}

			// Test Union, Inter, Diff
			switch gofakeit.Number(1, 3) {
			case 1:
				assert.Nil(db.SUnion(kp1.skey, kp2.skey, dstp.skey))
				assert.Nil(db.BitOr(kp1.bkey, kp2.bkey, dstp.bkey))

			case 2:
				assert.Nil(db.SInter(kp1.skey, kp2.skey, dstp.skey))
				assert.Nil(db.BitAnd(kp1.bkey, kp2.bkey, dstp.bkey))

			case 3:
				assert.Nil(db.SDiff(kp1.skey, kp2.skey, dstp.skey))
				assert.Nil(db.BitXor(kp1.bkey, kp2.bkey, dstp.bkey))
			}

			// Test Set error
			assert.Equal(db.SUnion(kp1.bkey, kp2.skey, ""), base.ErrWrongType)
			assert.Equal(db.SUnion(kp1.skey, kp2.bkey, ""), base.ErrWrongType)
			assert.Equal(db.SInter(kp1.bkey, kp2.skey, ""), base.ErrWrongType)
			assert.Equal(db.SInter(kp1.skey, kp2.bkey, ""), base.ErrWrongType)
			assert.Equal(db.SDiff(kp1.bkey, kp2.skey, ""), base.ErrWrongType)
			assert.Equal(db.SDiff(kp1.skey, kp2.bkey, ""), base.ErrWrongType)
			// Test Bitmap error
			assert.Equal(db.BitOr(kp1.bkey, kp2.skey, ""), base.ErrWrongType)
			assert.Equal(db.BitOr(kp1.skey, kp2.bkey, ""), base.ErrWrongType)
			assert.Equal(db.BitAnd(kp1.bkey, kp2.skey, ""), base.ErrWrongType)
			assert.Equal(db.BitAnd(kp1.skey, kp2.bkey, ""), base.ErrWrongType)
			assert.Equal(db.BitXor(kp1.bkey, kp2.skey, ""), base.ErrWrongType)
			assert.Equal(db.BitXor(kp1.skey, kp2.bkey, ""), base.ErrWrongType)
			// Test Bitmap other errors
			_, err = db.BitTest(kp1.skey, 100)
			assert.Equal(err, base.ErrWrongType)
			_, err = db.BitSet(kp1.skey, 100, true)
			assert.Equal(err, base.ErrWrongType)
			err = db.BitFlip(kp1.skey, 100)
			assert.Equal(err, base.ErrWrongType)
			_, err = db.BitArray(kp1.skey)
			assert.Equal(err, base.ErrWrongType)
			_, err = db.BitCount(kp1.skey)
			assert.Equal(err, base.ErrWrongType)

			setEqualBitmap(assert, db, kp1.skey, kp1.bkey)
			setEqualBitmap(assert, db, kp2.skey, kp2.bkey)
			setEqualBitmap(assert, db, dstp.skey, dstp.bkey)
		}
	}

	// err test
	db.Set("str", []byte(""))
	{
		// add
		err := db.SAdd("str", "foo")
		assert.Equal(err, base.ErrWrongType)
	}
	{
		// card
		res, err := db.SCard("str")
		assert.Equal(res, 0)
		assert.Equal(err, base.ErrWrongType)
	}
	{
		// has
		ok, err := db.SHas("str", "foo")
		assert.False(ok)
		assert.Equal(err, base.ErrWrongType)
	}
	{
		// remove
		ok, err := db.SRemove("str", "foo")
		assert.False(ok)
		assert.Equal(err, base.ErrWrongType)
	}
	{
		// members
		res, err := db.SMembers("str")
		var nilSlice []string
		assert.Equal(res, nilSlice)
		assert.Equal(err, base.ErrWrongType)
	}
	{
		// Union
		err := db.SUnion("str", "str", "")
		assert.Equal(err, base.ErrWrongType)
	}
}
