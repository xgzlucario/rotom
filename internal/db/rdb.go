package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"

	"github.com/xgzlucario/rotom/internal/iface"
)

// rdb file layout:
//
//	magic "ROTOM" | version u8 | count u64 | entries... | crc32 u32 (of everything before)
//	entry: objectType u8 | expireAt varint (unix ms, 0 = none) | key | value
const (
	rdbMagic   = "ROTOM"
	rdbVersion = 1
)

// SaveRDB writes dict to path atomically.
func SaveRDB(path string, dict *Dict) (err error) {
	writer := iface.NewWriter(make([]byte, 0, 4096))
	for _, c := range []byte(rdbMagic) {
		writer.WriteUint8(c)
	}
	writer.WriteUint8(rdbVersion)
	var count uint64
	dict.Range(func(string, any, int64) bool { count++; return true })
	writer.WriteUint64(count)

	dict.Range(func(k string, v any, expireAt int64) bool {
		enc := EncodingOf(v)
		writer.WriteUint8(uint8(enc))
		writer.WriteVarint(int(expireAt))
		writer.WriteString(k)

		if enc == EncodingString {
			writer.WriteBytes(v.([]byte))
		} else {
			v.(iface.Encoder).WriteTo(writer)
		}
		return true
	})
	writer.WriteUint32(crc32.ChecksumIEEE(writer.Bytes()))

	// write to a temp file, fsync, then atomically rename.
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
		}
	}()
	if _, err = f.Write(writer.Bytes()); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// LoadRDB reads an rdb file into a new Dict. A missing file yields an empty Dict.
func LoadRDB(path string) (dict *Dict, err error) {
	dict = NewDict()
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return dict, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return dict, nil
	}

	headerLen := len(rdbMagic) + 1
	if len(data) < headerLen+8+4 || string(data[:len(rdbMagic)]) != rdbMagic {
		return nil, errors.New("not a rotom rdb file")
	}
	if v := data[len(rdbMagic)]; v != rdbVersion {
		return nil, fmt.Errorf("unsupported rdb version %d", v)
	}
	body, sum := data[:len(data)-4], binary.LittleEndian.Uint32(data[len(data)-4:])
	if crc32.ChecksumIEEE(body) != sum {
		return nil, errors.New("rdb checksum mismatch")
	}

	// the decoders panic on malformed input; turn that into an error.
	defer func() {
		if r := recover(); r != nil {
			dict, err = nil, fmt.Errorf("rdb file corrupted: %v", r)
		}
	}()

	rd := iface.NewReader(body[headerLen:])
	n := rd.ReadUint64()
	for range n {
		enc := Encoding(rd.ReadUint8())
		expireAt := rd.ReadVarint()
		key := rd.ReadString()

		if enc == EncodingString {
			dict.SetWithExpire(key, rd.ReadBytes(), expireAt)
			continue
		}
		val, err := newEncoded(enc)
		if err != nil {
			return nil, err
		}
		val.ReadFrom(rd)
		dict.SetWithExpire(key, val, expireAt)
	}
	return dict, nil
}
