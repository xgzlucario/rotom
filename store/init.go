package store

import (
	"fmt"
	"log"
	"os"

	"github.com/xgzlucario/rotom/structx"
)

const (
	DB_MAX_COUNT = 16
)

var (
	// database store path
	StorePath = "db/"
)

type Store struct {
	id        int
	storePath string
	logger    *log.Logger
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
			storePath: fmt.Sprintf("%s%d.log", StorePath, i),
			m:         structx.NewCache[any](),
		}

		dbs[i].logger = NewLogger(dbs[i].storePath)

		// load
		dbs[i].load()
	}
}
