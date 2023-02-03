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
	storePath     = "db/"
	storeDuration = time.Second
)

type Store struct {
	id int                           // database id
	m  *structx.SyncMap[string, any] // data

	persist bool // persist enabled
}

// databases
var dbs []*Store

func init() {
	// init store dir
	os.Mkdir(storePath, os.ModeDir)

	dbs = make([]*Store, DB_MAX_COUNT)

	for i := range dbs {
		// default store
		dbs[i] = &Store{
			id:      i,
			m:       structx.NewSyncMap[any](),
			persist: true,
		}
		dbs[i].unmarshal()

		// backend
		go func(i int) {
			for {
				time.Sleep(storeDuration)
				dbs[i].marshal()
			}
		}(i)
	}
}
