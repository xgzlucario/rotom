package structx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"unsafe"

	"github.com/brianvoe/gofakeit/v6"
	jsoniter "github.com/json-iterator/go"
)

var m map[string]uint64

func init() {
	m = make(map[string]uint64)
	for i := 0; i < 100; i++ {
		m[gofakeit.UUID()] = gofakeit.Uint64()
	}
}

func BenchmarkJson(b *testing.B) {
	for i := 0; i < b.N; i++ {
		json.Marshal(m)
	}
}

func BenchmarkJsoniter(b *testing.B) {
	for i := 0; i < b.N; i++ {
		jsoniter.Marshal(m)
	}
}

// func BenchmarkSonic(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		sonic.Marshal(m)
// 	}
// }

func BenchmarkError(b *testing.B) {
	e1 := errors.New("error1")
	e2 := errors.New("error2")
	e3 := errors.New("error3")

	context.Background()

	type MyType struct{}
	a := new(MyType)
	fmt.Println(a, unsafe.Sizeof(a))

	eall := fmt.Errorf("%w,%w,%w", e1, e2, e3)

	fmt.Println(eall)
	fmt.Println(errors.Is(eall, e1))
}
 