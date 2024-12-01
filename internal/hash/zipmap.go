package hash

import (
	"encoding/binary"
	"github.com/cockroachdb/swiss"
	"github.com/xgzlucario/rotom/internal/iface"
	"github.com/xgzlucario/rotom/internal/pool"
	"github.com/xgzlucario/rotom/internal/resp"
)

const (
	migrateThresholdRate = 0.5
	migrateThresholdSize = 1024
)

var _ iface.MapI = (*ZipMap)(nil)

var (
	bpool = pool.NewBufferPool()
)

type ZipMap struct {
	unused int
	data   []byte
	index  *swiss.Map[string, uint32]
}

func New() *ZipMap {
	return &ZipMap{
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
	// update inplaced
	if ok {
		oldVal, n := zm.readVal(pos)
		if len(oldVal) == len(val) {
			copy(oldVal, val)
			return false
		}
		// mem trash
		zm.unused += n
		zm.Migrate()
	}
	zm.data = zm.appendKeyVal(zm.data, key, val)
	return !ok
}

func (zm *ZipMap) appendKeyVal(dst []byte, key string, val []byte) []byte {
	zm.index.Put(key, uint32(len(dst)))
	dst = binary.AppendUvarint(dst, uint64(len(val)))
	return append(dst, val...)
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
		// mem trash
		zm.unused += n
		zm.Migrate()
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

func (zm *ZipMap) Migrate() {
	if zm.unused < migrateThresholdSize {
		return
	}
	if float64(zm.unused)/float64(len(zm.data)) < migrateThresholdRate {
		return
	}
	newData := bpool.Get(len(zm.data))
	zm.Scan(func(key string, val []byte) {
		newData = zm.appendKeyVal(newData, key, val)
	})
	bpool.Put(zm.data)
	zm.data = newData
	zm.unused = 0
}

func (zm *ZipMap) Len() int { return zm.index.Len() }

func (zm *ZipMap) Encode(writer *resp.Writer) error {
	writer.WriteArray(zm.Len() * 2)
	zm.Scan(func(k string, v []byte) {
		writer.WriteBulkString(k)
		writer.WriteBulk(v)
	})
	return nil
}

func (zm *ZipMap) Decode(reader *resp.Reader) error {
	cmd, err := reader.ReadCommand()
	if err != nil {
		return err
	}
	for i := 0; i < len(cmd.Args); i += 2 {
		key := cmd.Args[i]
		val := cmd.Args[i+1]
		zm.Set(string(key), val)
	}
	return nil
}
