package store

import (
	"os"
	"time"

	"github.com/xgzlucario/rotom/structx"
)

const (
	DB_MAX_COUNT = 16
)

var (
	StorePath = "db/"
)

type Store struct {
	id int                           // database id
	m  *structx.SyncMap[string, any] // data

	persist       bool          // persist enabled
	storeDuration time.Duration // store duration
}

// databases
var dbs []*Store

func init() {
	// init store dir
	os.Mkdir(StorePath, os.ModeDir)

	dbs = make([]*Store, DB_MAX_COUNT)

	for i := range dbs {
		// default store
		dbs[i] = &Store{
			id:            i,
			m:             structx.NewSyncMap[any](),
			persist:       true,
			storeDuration: time.Second,
		}
		dbs[i].unmarshal()

		// backend
		go func(i int) {
			for {
				time.Sleep(dbs[i].storeDuration)
				dbs[i].marshal()
			}
		}(i)
	}
}
