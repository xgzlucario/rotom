package dict

import (
	"fmt"
)

func genKV(i int) (string, []byte) {
	k := fmt.Sprintf("%08x", i)
	return k, []byte(k)
}
