package store

import (
	"net"
	"time"

	cache "github.com/xgzlucario/GigaCache"
	"github.com/xgzlucario/rotom/base"
	"golang.org/x/exp/slices"
)

var (
	// Since there is a limit to the number of concurrent clients,
	// which is usually not very large (less than 512),
	// use bpool to reuse the buffer.
	bpool = cache.NewBytePoolCap(512, 1024, 1024)
)

// Client defines the client that connects to the server.
type Client struct {
	c net.Conn
	b []byte
}

// ResetBytePool
func ResetBytePool(maxSize, width, capwidth int) {
	bpool = cache.NewBytePoolCap(maxSize, width, capwidth)
}

// NewClient
func NewClient(addr string) (c *Client, err error) {
	c = &Client{}
	c.c, err = net.Dial("tcp", addr)
	return
}

// Set
func (c *Client) Set(key string, val []byte) (res []byte, err error) {
	return c.SetTx(key, val, NoTTL)
}

// SetEx
func (c *Client) SetEx(key string, val []byte, ttl time.Duration) (res []byte, err error) {
	return c.SetTx(key, val, cache.GetUnixNano()+int64(ttl))
}

// SetTx
func (c *Client) SetTx(key string, val []byte, ts int64) (res []byte, err error) {
	b := NewCodec(OpSetTx, 4).Type(TypeString).String(key).Int(ts).Bytes(val)
	res, err = c.send(b.Content())
	b.Recycle()
	return
}

// Get
func (c *Client) Get(key string) (res []byte, err error) {
	b := NewCodec(ReqGet, 1).String(key)
	res, err = c.send(b.Content())
	b.Recycle()
	return
}

// Len
func (c *Client) Len() (int64, error) {
	b := NewCodec(ReqLen, 0)
	bytes, err := c.send(b.Content())
	b.Recycle()
	if err != nil {
		return 0, err
	}
	return base.ParseInt[int64](bytes), nil
}

// Close
func (c *Client) Close() error {
	return c.c.Close()
}

// send post request and handle response.
func (c *Client) send(req []byte) ([]byte, error) {
	_, err := c.c.Write(req)
	if err != nil {
		return nil, err
	}
	c.b = bpool.Get()
	defer bpool.Put(c.b)

	n, err := c.c.Read(c.b)
	if err != nil {
		return nil, err
	}

	return slices.Clone(c.b[:n]), nil
}
