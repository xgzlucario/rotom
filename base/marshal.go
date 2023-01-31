package base

import "github.com/bytedance/sonic"

func MarshalJSON(data any) ([]byte, error) {
	return sonic.Marshal(data)
}

func UnmarshalJSON(src []byte, data any) error {
	return sonic.Unmarshal(src, data)
}
