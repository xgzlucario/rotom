package test

import (
	"bytes"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	cache "github.com/xgzlucario/GigaCache"
	"github.com/xgzlucario/rotom/store"
)

type vItem struct {
	Val []byte
	Ts  int64
}

// Test cache set operation
func TestCacheSet(t *testing.T) {
	cfg := store.DefaultConfig
	dbkey := gofakeit.UUID()
	cfg.Path = dbkey + ".db"

	db, err := store.Open(store.DefaultConfig)
	if err != nil {
		panic(err)
	}

	// generate test data
	num := 10000 * 100
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
			if ok || val != nil || ts != 0 {
				t.Fatal("key should not exist")
			}

		} else {
			val, ts, ok := db.Get(k)
			if !ok || !bytes.Equal(val, v.Val) || ts != v.Ts {
				t.Fatalf("set: %v %v %v get: %v %v %v",
					k, string(v.Val), v.Ts,
					string(val), ts, ok)
			}
		}
	}

	time.Sleep(time.Second * 3)
	db.Close()

	// load
	db, err = store.Open(cfg)
	if err != nil {
		panic(err)
	}

	// get again
	for k, v := range kvdata {
		// timeCarry convert
		v.Ts /= (1000 * 1000 * 1000)
		v.Ts *= (1000 * 1000 * 1000)

		// expired
		if v.Ts < cache.GetUnixNano() {
			_, _, ok := db.Get(k)
			if ok {
				t.Fatal("key should not exist")
			}

		} else {
			val, ts, ok := db.Get(k)
			if !ok || !bytes.Equal(val, v.Val) || ts != v.Ts {
				t.Fatalf("set: %v %v %v get: %v %v %v",
					k, string(v.Val), v.Ts,
					string(val), ts, ok)
			}
		}
	}
}

// TestBitmap
func TestBitmap(t *testing.T) {
	cfg := store.DefaultConfig
	dbkey := gofakeit.UUID()
	cfg.Path = dbkey + ".db"

	db, err := store.Open(store.DefaultConfig)
	if err != nil {
		panic(err)
	}

	db.BitSet("bm", 1, true)
	db.BitSet("bm", 5, true)
	db.BitSet("bm", 9, true)

	test := func() {
		// false
		r, err := db.BitTest("bm", 4)
		if r || err != nil {
			t.Fatal("bit count error")
		}

		// true
		r, err = db.BitTest("bm", 5)
		if !r || err != nil {
			t.Fatal("bit count error")
		}

		// 3
		c, err := db.BitCount("bm")
		if c != 3 || err != nil {
			t.Fatal("bit count error")
		}
	}

	test()

	time.Sleep(time.Second * 3)
	db.Close()

	// load
	db, err = store.Open(cfg)
	if err != nil {
		panic(err)
	}

	test()
}

func FuzzTest(f *testing.F) {
	db, err := store.Open(store.DefaultConfig)
	if err != nil {
		panic(err)
	}

	f.Fuzz(func(t *testing.T, key string, val []byte, ts int64) {
		db.SetTx(key, val, ts)
		now := cache.GetUnixNano()

		v, ttl, ok := db.Get(key)

		// no ttl
		if ts == 0 {
			if v == nil || ttl != 0 || !ok {
				t.Fatalf("[0] set: %v %v %v get: %v %v %v", key, val, ts, v, ttl, ok)
			}

			// expired
		} else if ts < now {
			if v != nil || ttl != 0 || ok {
				t.Fatalf("[1] set: %v %v %v get: %v %v %v", key, val, ts, v, ttl, ok)
			}

			// not expired
		} else if ts > now {
			if bytes.Equal(v, val) || ts != ttl || !ok {
				t.Fatalf("[2] set: %v %v %v get: %v %v %v", key, val, ts, v, ttl, ok)
			}
		}
	})
}
