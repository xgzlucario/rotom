package rotom

import (
	"errors"
	"net"
	"strconv"
	"time"

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

// Ping
func (c *Client) Ping() ([]byte, error) {
	return c.do(NewCodec(OpPing))
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

// Incr
func (c *Client) Incr(key string, val float64) (float64, error) {
	args, err := c.do(NewCodec(OpIncr).Str(key).Float(val))
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(string(args), 64)
}

// Remove
func (c *Client) Remove(keys ...string) (int, error) {
	args, err := c.do(NewCodec(OpRemove).StrSlice(keys))
	if err != nil {
		return 0, err
	}
	return args.ToInt(), nil
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
	return args.ToUint64(), nil
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
	return args.ToInt(), nil
}

// HKeys
func (c *Client) HKeys(key string) ([]string, error) {
	args, err := c.do(NewCodec(OpHKeys).Str(key))
	if err != nil {
		return nil, err
	}
	return args.ToStrSlice(), err
}

// HRemove
func (c *Client) HRemove(key, field string) (bool, error) {
	args, err := c.do(NewCodec(OpHRemove).Str(key).Str(field))
	if err != nil {
		return false, err
	}
	return args.ToBool(), nil
}

// SAdd Append items into set, and returns the number of new items added.
func (c *Client) SAdd(key string, items ...string) (int, error) {
	args, err := c.do(NewCodec(OpSAdd).Str(key).StrSlice(items))
	if err != nil {
		return 0, err
	}
	return args.ToInt(), nil
}

// SRemove
func (c *Client) SRemove(key, item string) error {
	return c.doNoRes(NewCodec(OpSRemove).Str(key).Str(item))
}

// SPop
func (c *Client) SPop(key string) (string, error) {
	args, err := c.do(NewCodec(OpSPop).Str(key))
	if err != nil {
		return "", err
	}
	return args.ToStr(), nil
}

// SHas
func (c *Client) SHas(key, item string) (bool, error) {
	args, err := c.do(NewCodec(OpSHas).Str(key).Str(item))
	if err != nil {
		return false, err
	}
	return args.ToBool(), nil
}

// SCard
func (c *Client) SCard(key string) (int, error) {
	args, err := c.do(NewCodec(OpSCard).Str(key))
	if err != nil {
		return 0, err
	}
	return args.ToInt(), nil
}

// SMembers
func (c *Client) SMembers(key string) ([]string, error) {
	args, err := c.do(NewCodec(OpSMembers).Str(key))
	if err != nil {
		return nil, err
	}
	return args.ToStrSlice(), nil
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

// LPush
func (c *Client) LPush(key, item string) error {
	return c.doNoRes(NewCodec(OpLPush).Str(key).Str(item))
}

// LPop
func (c *Client) LPop(key string) (string, error) {
	res, err := c.do(NewCodec(OpLPop).Str(key))
	if err != nil {
		return "", err
	}
	return string(res), nil
}

// RPush
func (c *Client) RPush(key, item string) error {
	return c.doNoRes(NewCodec(OpRPush).Str(key).Str(item))
}

// RPop
func (c *Client) RPop(key string) (string, error) {
	res, err := c.do(NewCodec(OpRPop).Str(key))
	if err != nil {
		return "", err
	}
	return string(res), nil
}

// LLen
func (c *Client) LLen(key string) (int, error) {
	args, err := c.do(NewCodec(OpLLen).Str(key))
	if err != nil {
		return 0, err
	}
	return args.ToInt(), nil
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
	return args.ToBool(), nil
}

// BitFlip
func (c *Client) BitFlip(key string, offset uint32) error {
	return c.doNoRes(NewCodec(OpBitFlip).Str(key).Uint(offset))
}

// BitOr
func (c *Client) BitOr(dstKey string, srcKeys ...string) error {
	return c.doNoRes(NewCodec(OpBitOr).Str(dstKey).StrSlice(srcKeys))
}

// BitAnd
func (c *Client) BitAnd(dstKey string, srcKeys ...string) error {
	return c.doNoRes(NewCodec(OpBitAnd).Str(dstKey).StrSlice(srcKeys))
}

// BitXor
func (c *Client) BitXor(dstKey string, srcKeys ...string) error {
	return c.doNoRes(NewCodec(OpBitXor).Str(dstKey).StrSlice(srcKeys))
}

// BitCount
func (c *Client) BitCount(key string) (uint64, error) {
	args, err := c.do(NewCodec(OpBitCount).Str(key))
	if err != nil {
		return 0, err
	}
	return args.ToUint64(), nil
}

// BitArray
func (c *Client) BitArray(key string) ([]uint32, error) {
	res, err := c.do(NewCodec(OpBitArray).Str(key))
	if err != nil {
		return nil, err
	}
	return res.ToUint32Slice(), nil
}

// ZAdd
func (c *Client) ZAdd(key, field string, score float64, val []byte) error {
	return c.doNoRes(NewCodec(OpZAdd).Str(key).Str(field).Float(score).Bytes(val))
}

// ZIncr
func (c *Client) ZIncr(key, field string, score float64) (float64, error) {
	args, err := c.do(NewCodec(OpZIncr).Str(key).Str(field).Float(score))
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(string(args), 64)
}

// ZRemove
func (c *Client) ZRemove(key, field string) error {
	return c.doNoRes(NewCodec(OpZRemove).Str(key).Str(field))
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
func (c *Client) do(cd *Codec) (Result, error) {
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
	if args[0].ToInt64() == RES_ERROR {
		return nil, errors.New(string(args[1]))
	}

	return args[1], nil
}
