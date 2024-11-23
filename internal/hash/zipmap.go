package hash

import (
	"bytes"
	"encoding/binary"
	"github.com/cockroachdb/swiss"
	"github.com/xgzlucario/rotom/internal/iface"
	"github.com/xgzlucario/rotom/internal/resp"
)

var _ iface.MapI = (*ZipMap)(nil)

type ZipMap struct {
	unused uint32
	data   []byte
	index  *swiss.Map[string, uint32]
}

func New() *ZipMap {
	return &ZipMap{
		data:  make([]byte, 0, 64),
		index: swiss.New[string, uint32](8),
	}
}

func (zm *ZipMap) Get(key string) ([]byte, bool) {
	pos, ok := zm.index.Get(key)
	if !ok {
		return nil, false
	}
	val, _ := zm.readVal(pos)
	return val, true
}

func (zm *ZipMap) Set(key string, val []byte) bool {
	pos, ok := zm.index.Get(key)
	// update inplace
	if ok {
		oldVal, n := zm.readVal(pos)
		if len(oldVal) == len(val) {
			copy(oldVal, val)
			return false
		}
		// mem trash
		zm.unused += uint32(n)
	}
	zm.index.Put(key, uint32(len(zm.data)))
	zm.data = binary.AppendUvarint(zm.data, uint64(len(val)))
	zm.data = append(zm.data, val...)
	return !ok
}

func (zm *ZipMap) readVal(pos uint32) ([]byte, int) {
	data := zm.data[pos:]
	vlen, n := binary.Uvarint(data)
	val := data[n : n+int(vlen)]
	return val, n + int(vlen)
}

func (zm *ZipMap) Remove(key string) bool {
	pos, ok := zm.index.Get(key)
	if ok {
		zm.index.Delete(key)
		_, n := zm.readVal(pos)
		zm.unused += uint32(n)
	}
	return ok
}

func (zm *ZipMap) Scan(fn func(string, []byte)) {
	zm.index.All(func(key string, pos uint32) bool {
		val, _ := zm.readVal(pos)
		fn(key, val)
		return true
	})
}

func (zm *ZipMap) Len() int {
	return zm.index.Len()
}

func (zm *ZipMap) Compress() {

}

func (zm *ZipMap) Encode(writer *resp.Writer) error {
	writer.WriteArrayHead(zm.Len())
	zm.Scan(func(k string, v []byte) {
		writer.WriteBulkString(k)
		writer.WriteBulk(v)
	})
	return nil
}

func (zm *ZipMap) Decode(reader *resp.Reader) error {
	n, err := reader.ReadArrayHead()
	if err != nil {
		return err
	}
	*zm = *New()
	for range n {
		key, err := reader.ReadBulk()
		if err != nil {
			return err
		}
		val, err := reader.ReadBulk()
		if err != nil {
			return err
		}
		zm.Set(string(key), bytes.Clone(val))
	}
	return nil
}
