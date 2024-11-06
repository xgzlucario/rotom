package iface

import "github.com/xgzlucario/rotom/internal/resp"

type Encoder interface {
	Encode(writer *resp.Writer) error
	Decode(reader *resp.Reader) error
}
