package rotom

import "github.com/xgzlucario/rotom/codeman"

// NewCodec
func NewCodec(op Operation) *codeman.Codec {
	return codeman.NewCodec().Byte(byte(op))
}

// ParseRecord
func ParseRecord(decoder *codeman.Decoder) (Operation, []codeman.Result, error) {
	sop, err := decoder.Parse()
	if err != nil {
		return 0, nil, err
	}

	op := sop.ToInt()
	argsNum := cmdTable[op].argsNum

	args, err := decoder.Parses(argsNum)
	if err != nil {
		return 0, nil, err
	}

	return Operation(op), args, nil
}
