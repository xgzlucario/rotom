// Package db implements the keyspace: values, expiration and RDB files.
package db

// DB is the database commands operate on. It wraps the Dict so that FLUSHDB
// and LOAD can replace the whole keyspace.
type DB struct {
	*Dict
}

func New() *DB { return &DB{Dict: NewDict()} }
