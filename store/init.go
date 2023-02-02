package store

import (
	"os"
	"time"

	"github.com/xgzlucario/rotom/structx"
)

const (
	DB_MAX_COUNT   = 16
	STORE_DURATION = time.Second
)

var (
	StorePath = "db/"
)

type Store struct {
	id int                           // database id
	m  *structx.SyncMap[string, any] // data

	Persist bool // enable persist
}

// databases
var dbs []*Store

func init() {
	os.Mkdir(StorePath, os.ModeDir)

	dbs = make([]*Store, DB_MAX_COUNT)

	for i := range dbs {
		dbs[i] = &Store{i, structx.NewSyncMap[any](), true}
		dbs[i].unmarshal()

		// backend
		go func(i int) {
			for {
				time.Sleep(STORE_DURATION)
				dbs[i].marshal()
			}
		}(i)
	}
}
