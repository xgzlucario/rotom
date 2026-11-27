package hash

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/xgzlucario/rotom/internal/codec"
)

func toMap(zm *ZipMap) map[string]string {
	res := map[string]string{}
	for f, v := range zm.All() {
		res[f] = string(v)
	}
	return res
}

func TestModel(t *testing.T) {
	ast := assert.New(t)
	zm := New()
	model := map[string]string{}
	for i := range 50000 {
		f := fmt.Sprint(rand.IntN(500))
		switch rand.IntN(4) {
		case 0, 1:
			v := strings.Repeat("v", rand.IntN(200))
			_, exists := model[f]
			ast.Equal(!exists, zm.Set(f, []byte(v)))
			model[f] = v
		case 2:
			v, ok := zm.Get(f)
			want, exists := model[f]
			ast.Equal(exists, ok)
			ast.Equal(want, string(v))
		case 3:
			_, exists := model[f]
			ast.Equal(exists, zm.Remove(f))
			delete(model, f)
		}
		if i%5000 == 0 {
			ast.Equal(model, toMap(zm))
		}
	}
	ast.Equal(model, toMap(zm))

	w := codec.NewWriter(nil)
	zm.Encode(w)
	zm2 := New()
	zm2.Decode(codec.NewReader(w.Bytes()))
	ast.Equal(model, toMap(zm2))
}

// Compaction keeps garbage bounded under constant overwrites.
func TestCompaction(t *testing.T) {
	ast := assert.New(t)
	zm := New()
	for round := range 1000 {
		for i := range 100 {
			zm.Set(fmt.Sprint(i), make([]byte, 1+round%2))
		}
	}
	live := 0
	for _, v := range zm.All() {
		live += 1 + len(v)
	}
	ast.LessOrEqual(len(zm.data), 2*live+2*compactMinUnused)
	ast.Len(maps.Collect(zm.All()), 100)
}

// The field passed to Set may alias a reused network buffer.
func TestFieldIsCopied(t *testing.T) {
	buf := []byte("field")
	zm := New()
	zm.Set(unsafe.String(&buf[0], len(buf)), []byte("v"))
	zm.Set(unsafe.String(&buf[0], len(buf)), []byte("longer")) // overwrite, different length
	copy(buf, "xxxxx")
	_, ok := zm.Get("field")
	assert.True(t, ok)
}

func BenchmarkZipMap(b *testing.B) {
	zm := New()
	for i := range 512 {
		zm.Set(fmt.Sprintf("%08x", i), []byte("value"))
	}
	b.Run("get", func(b *testing.B) {
		for b.Loop() {
			zm.Get("000000ff")
		}
	})
	b.Run("set-inplace", func(b *testing.B) {
		v := []byte("VALUE")
		for b.Loop() {
			zm.Set("000000ff", v)
		}
	})
}
