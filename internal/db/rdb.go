package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"

	"github.com/xgzlucario/rotom/internal/codec"
	"github.com/xgzlucario/rotom/internal/fileutil"
)

// rdb file layout:
//
//	magic "ROTOM" | version u8 | count uvarint | entries... | crc32 u32 (of everything before)
//	entry: encoding u8 | expireAt varint (unix ms, 0 = none) | key | value
const (
	rdbMagic   = "ROTOM"
	rdbVersion = 2
)

// SaveRDB writes dict to path atomically.
func SaveRDB(path string, dict *Dict) error {
	return fileutil.WriteAtomic(path, EncodeRDB(dict))
}

// EncodeRDB returns the rdb encoding of dict. AOF rewrites use it as the base
// file, so it must be a consistent snapshot: it never modifies the dict.
func EncodeRDB(dict *Dict) []byte {
	buf := append(make([]byte, 0, 4096), rdbMagic...)
	w := codec.NewWriter(append(buf, rdbVersion))

	count := 0
	for range dict.live() {
		count++
	}
	w.WriteUvarint(uint64(count))
	for e := range dict.live() {
		v := e.value()
		enc := EncodingOf(v)
		w.WriteUint8(uint8(enc))
		w.WriteVarint(e.expireAt)
		w.WriteString(e.key)
		if enc == EncodingString {
			w.WriteBytes(e.str)
		} else {
			v.(codec.Encoder).Encode(w)
		}
	}
	w.WriteUint32(crc32.ChecksumIEEE(w.Bytes()))
	return w.Bytes()
}

// LoadRDB reads an rdb file into a new Dict. A missing file yields an empty Dict.
func LoadRDB(path string) (dict *Dict, err error) {
	dict = NewDict()
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) || (err == nil && len(data) == 0) {
		return dict, nil
	}
	if err != nil {
		return nil, err
	}

	header := len(rdbMagic) + 1
	if len(data) < header+4 || string(data[:len(rdbMagic)]) != rdbMagic {
		return nil, errors.New("not a rotom rdb file")
	}
	if v := data[len(rdbMagic)]; v != rdbVersion {
		return nil, fmt.Errorf("unsupported rdb version %d (want %d)", v, rdbVersion)
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

	r := codec.NewReader(body[header:])
	for range r.ReadLen() {
		enc := Encoding(r.ReadUint8())
		expireAt := r.ReadVarint()
		key := r.ReadString()
		if dict.data.Get(key) != nil {
			return nil, fmt.Errorf("duplicate key %q", key)
		}
		e := dict.insert(key)
		dict.setExpire(e, expireAt)
		if enc == EncodingString {
			e.str = r.ReadBytes()
			continue
		}
		obj, err := newObject(enc)
		if err != nil {
			return nil, err
		}
		obj.Decode(r)
		e.obj = obj
	}
	if r.Len() != 0 {
		return nil, errors.New("rdb file has trailing data")
	}
	return dict, nil
}
