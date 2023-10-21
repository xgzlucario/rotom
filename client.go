package rotom

import (
	"net"
	"time"

	cache "github.com/xgzlucario/GigaCache"
	"github.com/xgzlucario/rotom/base"
	"golang.org/x/exp/slices"
)

var (
	// Since there is a limit to the number of concurrent clients,
	// which is usually not very large,
	// use bpool to reuse the buffer.
	bpool = cache.NewBytePoolCap(1000, 512, 512)
)

// Client defines the client that connects to the server.
type Client struct {
	c net.Conn
	b []byte
}

// NewClient
func NewClient(addr string) (c *Client, err error) {
	c = &Client{}
	c.c, err = net.Dial("tcp", addr)
	return
}

// Set
func (c *Client) Set(key string, val []byte) ([]byte, error) {
	return c.SetTx(key, val, NoTTL)
}

// SetEx
func (c *Client) SetEx(key string, val []byte, ttl time.Duration) ([]byte, error) {
	return c.SetTx(key, val, cache.GetClock()+int64(ttl))
}

// SetTx
func (c *Client) SetTx(key string, val []byte, ts int64) ([]byte, error) {
	return c.do(NewCodec(OpSetTx).Type(TypeString).Str(key).Int(ts).Bytes(val))
}

// Remove
func (c *Client) Remove(key string) ([]byte, error) {
	return c.do(NewCodec(OpRemove).Str(key))
}

// Rename
func (c *Client) Rename(key, newKey string) ([]byte, error) {
	return c.do(NewCodec(OpRename).Str(key).Str(newKey))
}

// Get
func (c *Client) Get(key string) ([]byte, error) {
	return c.do(NewCodec(ReqGet).Str(key))
}

// Len
func (c *Client) Len() (int, error) {
	bytes, err := c.do(NewCodec(ReqLen))
	if err != nil {
		return 0, err
	}
	return base.ParseInt[int](bytes), nil
}

// HSet
func (c *Client) HSet(key, field string, val []byte) ([]byte, error) {
	return c.do(NewCodec(OpHSet).Str(key).Str(field).Bytes(val))
}

// HRemove
func (c *Client) HRemove(key, field string) ([]byte, error) {
	return c.do(NewCodec(OpHRemove).Str(key).Str(field))
}

// Close
func (c *Client) Close() error {
	return c.c.Close()
}

// do send request and return response.
func (c *Client) do(cd *Codec) ([]byte, error) {
	_, err := c.c.Write(cd.B)
	cd.Recycle()

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
