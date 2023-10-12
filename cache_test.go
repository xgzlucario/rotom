package rotom

import (
	"bytes"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	cache "github.com/xgzlucario/GigaCache"
)

type vItem struct {
	Val []byte
	Ts  int64
}

// Test cache set operation
func TestCacheSet(t *testing.T) {
	cfg := DefaultConfig
	cfg.Path = gofakeit.UUID() + ".db"

	db, err := Open(cfg)
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
			val, ts, ok := db.GetBytes(k)
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
	db, err = Open(cfg)
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
			val, ts, ok := db.GetBytes(k)
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
	cfg := DefaultConfig
	cfg.Path = gofakeit.UUID() + ".db"

	db, err := Open(cfg)
	if err != nil {
		panic(err)
	}

	// valid map
	const num = 100 * 10000
	vmap := map[uint32]struct{}{}

	for i := 0; i < num; i++ {
		offset := gofakeit.Uint32()

		vmap[offset] = struct{}{}
		db.BitSet("bm", offset, true)
	}

	// len
	if c, err := db.BitCount("bm"); c != uint64(len(vmap)) || err != nil {
		t.Fatal("bit len error")
	}

	test := func() {
		for i := uint32(0); i < num; i++ {
			_, ok := vmap[i]
			ok2, err := db.BitTest("bm", i)

			if ok != ok2 || err != nil {
				t.Fatal("bit count error")
			}
		}
	}

	test()

	time.Sleep(time.Second * 3)
	db.Close()

	// load
	db, err = Open(cfg)
	if err != nil {
		panic(err)
	}

	test()
}

func FuzzTest(f *testing.F) {
	cfg := DefaultConfig
	cfg.Path = gofakeit.UUID() + ".db"

	db, err := Open(cfg)
	if err != nil {
		panic(err)
	}

	f.Fuzz(func(t *testing.T, key string, val []byte, ts int64) {
		db.SetTx(key, val, ts)
		now := cache.GetUnixNano()

		v, ttl, ok := db.GetBytes(key)

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
