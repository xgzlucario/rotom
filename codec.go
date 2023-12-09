package rotom

import "github.com/xgzlucario/rotom/codeman"

// NewCodec
func NewCodec(op Operation) *codeman.Codec {
	return codeman.NewCodec().Byte(byte(op))
}
