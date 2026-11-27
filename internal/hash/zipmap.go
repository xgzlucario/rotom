package hash

import (
	"encoding/binary"
	"github.com/xgzlucario/rotom/internal/iface"
	"github.com/xgzlucario/rotom/internal/pool"
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
	index  map[string]uint32
}

func New() *ZipMap {
	return &ZipMap{
		index: make(map[string]uint32, 8),
	}
}

func (zm *ZipMap) Get(key string) ([]byte, bool) {
	pos, ok := zm.index[key]
	if !ok {
		return nil, false
	}
	val, _ := zm.readVal(pos)
	return val, true
}

func (zm *ZipMap) Set(key string, val []byte) bool {
	pos, ok := zm.index[key]
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
	zm.index[key] = uint32(len(dst))
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
	pos, ok := zm.index[key]
	if ok {
		delete(zm.index, key)
		_, n := zm.readVal(pos)
		// mem trash
		zm.unused += n
		zm.Migrate()
	}
	return ok
}

func (zm *ZipMap) Scan(fn func(string, []byte)) {
	for key, pos := range zm.index {
		val, _ := zm.readVal(pos)
		fn(key, val)
	}
}

func (zm *ZipMap) Migrate() {
	if zm.unused < migrateThresholdSize {
		return
	}
	if float64(zm.unused)/float64(len(zm.data)) < migrateThresholdRate {
		return
	}
	newData := bpool.Get(len(zm.data) - zm.unused)[:0]
	zm.Scan(func(key string, val []byte) {
		newData = zm.appendKeyVal(newData, key, val)
	})
	bpool.Put(zm.data)
	zm.data = newData
	zm.unused = 0
}

func (zm *ZipMap) Len() int { return len(zm.index) }

func (zm *ZipMap) ReadFrom(rd *iface.Reader) {
	zm.unused = int(rd.ReadUint64())
	zm.data = rd.ReadBytes()
	n := rd.ReadUint64()
	for range n {
		zm.index[rd.ReadString()] = rd.ReadUint32()
	}
}

// WriteTo encode zipmap to [unused, data, indexLen, key1, pos1, ...].
func (zm *ZipMap) WriteTo(w *iface.Writer) {
	w.WriteUint64(uint64(zm.unused))
	w.WriteBytes(zm.data)
	w.WriteUint64(uint64(len(zm.index)))
	for key, pos := range zm.index {
		w.WriteString(key)
		w.WriteUint32(pos)
	}
}
