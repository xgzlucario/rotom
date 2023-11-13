package rotom

import "github.com/xgzlucario/rotom/codeman"

// NewCodec
func NewCodec(op Operation) *codeman.Codec {
	c := codeman.NewCodec()
	return c.Byte(byte(op))
}

// ParseRecord
func ParseRecord(decoder *codeman.Decoder) (Operation, []codeman.Result, error) {
	sop, err := decoder.ParseOne()
	if err != nil {
		return 0, nil, err
	}

	op := sop.ToInt()
	argsNum := cmdTable[op].argsNum
	// if args is expty.
	if argsNum == 0 {
		return Operation(op), nil, nil
	}

	args, err := decoder.Parse(argsNum)
	if err != nil {
		return 0, nil, err
	}

	return Operation(op), args, nil
}
