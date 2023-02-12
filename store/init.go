package store

import (
	"fmt"
	"os"
	"time"

	"github.com/xgzlucario/rotom/structx"
)

const (
	DB_MAX_COUNT = 16
)

var (
	// database store path
	StorePath = "db/"

	// datatbase store duration
	StoreDuration = time.Second

	// enabled persist
	Persist = true
)

type Store struct {
	id        int
	storePath string
	m         *structx.Cache[string, any]
}

// databases
var dbs []*Store

func init() {
	// init store dir
	if err := os.MkdirAll(StorePath, os.ModeDir); err != nil {
		panic(err)
	}

	dbs = make([]*Store, DB_MAX_COUNT)

	for i := range dbs {
		// init
		dbs[i] = &Store{
			id:        i,
			storePath: fmt.Sprintf("%s%d.bin", StorePath, i),
			m:         structx.NewCache[any](),
		}

		// load
		dbs[i].unmarshal()

		// save
		go func(i int) {
			for {
				time.Sleep(StoreDuration)
				dbs[i].marshal()
			}
		}(i)
	}
}
