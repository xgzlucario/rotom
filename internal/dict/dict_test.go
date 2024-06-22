package dict

import (
	"fmt"
)

func genKV(i int) (string, []byte) {
	k := fmt.Sprintf("%09x", i)
	return k, []byte(k)
}
