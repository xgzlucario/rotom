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
	c.b = make([]byte, 4096)
	return
}

// Set
func (c *Client) Set(key string, val []byte) error {
	return c.SetTx(key, val, noTTL)
}

// SetEx
func (c *Client) SetEx(key string, val []byte, ttl time.Duration) error {
	return c.SetTx(key, val, cache.GetClock()+int64(ttl))
}

// SetTx
func (c *Client) SetTx(key string, val []byte, ts int64) error {
	return c.doNoRes(NewCodec(OpSetTx).Type(TypeString).Str(key).Int(ts / timeCarry).Bytes(val))
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
	return c.doNoRes(NewCodec(OpHSet).Str(key).Str(field).Bytes(val))
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

// SAdd Append items into set, and returns the number of new items added.
func (c *Client) SAdd(key string, items ...string) (int, error) {
	args, err := c.do(NewCodec(OpSAdd).Str(key).StrSlice(items))
	if err != nil {
		return 0, err
	}
	return base.ParseInt[int](args), nil
}

// SRemove
func (c *Client) SRemove(key, item string) error {
	return c.doNoRes(NewCodec(OpSRemove).Str(key).Str(item))
}

// SHas
func (c *Client) SHas(key, item string) (bool, error) {
	args, err := c.do(NewCodec(OpSHas).Str(key).Str(item))
	if err != nil {
		return false, err
	}
	return args[0] == _true, nil
}

// SCard
func (c *Client) SCard(key string) (int, error) {
	args, err := c.do(NewCodec(OpSCard).Str(key))
	if err != nil {
		return 0, err
	}
	return base.ParseInt[int](args), nil
}

// SMembers
func (c *Client) SMembers(key string) ([]string, error) {
	args, err := c.do(NewCodec(OpSMembers).Str(key))
	if err != nil {
		return nil, err
	}
	return base.ParseStrSlice(args), nil
}

// SUnion
func (c *Client) SUnion(dstKey string, srcKeys ...string) error {
	return c.doNoRes(NewCodec(OpSUnion).Str(dstKey).StrSlice(srcKeys))
}

// SInter
func (c *Client) SInter(dstKey string, srcKeys ...string) error {
	return c.doNoRes(NewCodec(OpSInter).Str(dstKey).StrSlice(srcKeys))
}

// SDiff
func (c *Client) SDiff(dstKey string, srcKeys ...string) error {
	return c.doNoRes(NewCodec(OpSDiff).Str(dstKey).StrSlice(srcKeys))
}

// BitSet
func (c *Client) BitSet(key string, offset uint32, val bool) error {
	return c.doNoRes(NewCodec(OpBitSet).Str(key).Uint(offset).Bool(val))
}

// BitTest
func (c *Client) BitTest(key string, offset uint32) (bool, error) {
	args, err := c.do(NewCodec(OpBitTest).Str(key).Uint(offset))
	if err != nil {
		return false, err
	}
	return args[0] == _true, nil
}

// BitFlip
func (c *Client) BitFlip(key string, offset uint32) error {
	return c.doNoRes(NewCodec(OpBitFlip).Str(key).Uint(offset))
}

// Close
func (c *Client) Close() error {
	return c.c.Close()
}

// doNoRes do without res.
func (c *Client) doNoRes(cd *Codec) error {
	_, err := c.do(cd)
	return err
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
