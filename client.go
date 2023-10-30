package rotom

import (
	"errors"
	"net"
	"time"

	"github.com/bytedance/sonic"
	cache "github.com/xgzlucario/GigaCache"
	"github.com/xgzlucario/rotom/base"
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
	c.b = make([]byte, 512)
	return
}

// Set
func (c *Client) Set(key string, val []byte) ([]byte, error) {
	return c.SetTx(key, val, noTTL)
}

// SetEx
func (c *Client) SetEx(key string, val []byte, ttl time.Duration) ([]byte, error) {
	return c.SetTx(key, val, cache.GetClock()+int64(ttl))
}

// SetTx
func (c *Client) SetTx(key string, val []byte, ts int64) ([]byte, error) {
	return c.do(NewCodec(OpSetTx).Type(TypeString).Str(key).Int(ts / timeCarry).Bytes(val))
}

// Remove
func (c *Client) Remove(key string) (bool, error) {
	args, err := c.do(NewCodec(OpRemove).Str(key))
	if err != nil {
		return false, err
	}
	return args[0] == _true, nil
}

// Rename
func (c *Client) Rename(key, newKey string) (bool, error) {
	args, err := c.do(NewCodec(OpRename).Str(key).Str(newKey))
	if err != nil {
		return false, err
	}
	return args[0] == _true, nil
}

// Get
func (c *Client) Get(key string) ([]byte, error) {
	return c.do(NewCodec(OpGet).Str(key))
}

// Len
func (c *Client) Len() (uint64, error) {
	args, err := c.do(NewCodec(OpLen))
	if err != nil {
		return 0, err
	}
	return base.ParseInt[uint64](args), nil
}

// HSet
func (c *Client) HSet(key, field string, val []byte) error {
	_, err := c.do(NewCodec(OpHSet).Str(key).Str(field).Bytes(val))
	return err
}

// HGet
func (c *Client) HGet(key, field string) ([]byte, error) {
	return c.do(NewCodec(OpHGet).Str(key).Str(field))
}

// HLen
func (c *Client) HLen(key string) (int, error) {
	args, err := c.do(NewCodec(OpHLen).Str(key))
	if err != nil {
		return 0, err
	}
	return base.ParseInt[int](args), nil
}

// HKeys
func (c *Client) HKeys(key string) ([]string, error) {
	args, err := c.do(NewCodec(OpHKeys).Str(key))
	if err != nil {
		return nil, err
	}
	var keys []string
	err = sonic.Unmarshal(args, &keys)
	return keys, err
}

// HRemove
func (c *Client) HRemove(key, field string) (bool, error) {
	args, err := c.do(NewCodec(OpHRemove).Str(key).Str(field))
	if err != nil {
		return false, err
	}
	return args[0] == _true, nil
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

	// read response.
	n, err := c.c.Read(c.b)
	if err != nil {
		return nil, err
	}

	// parse data.
	op, args, err := NewDecoder(c.b[:n]).ParseRecord()
	if err != nil {
		return nil, err
	}
	if op != Response {
		return nil, base.ErrInvalidResponse
	}

	// the first args is response code.
	if int64(args[0][0]) == RES_ERROR {
		return nil, errors.New(*b2s(args[1]))
	}

	return args[1], nil
}
