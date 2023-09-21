package main

import (
	"net"

	"github.com/xgzlucario/rotom/store"
)

type Client struct {
	conn net.Conn
}

// NewClient
func NewClient(addr string) (c *Client, err error) {
	c = &Client{}
	c.conn, err = net.Dial("tcp", addr)
	return
}

// Set
func (c *Client) Set(key string, val []byte) (res []byte, err error) {
	cd := store.NewCodec(store.OpSetTx, 4).
		Type(store.TypeString).String(key).
		Int(0).Bytes(val)

	send(c.conn, cd.Content(), func(r []byte) error {
		res = r
		return nil
	})

	cd.Recycle()
	return
}
