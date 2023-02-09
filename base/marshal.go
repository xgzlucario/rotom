package base

import jsoniter "github.com/json-iterator/go"

func MarshalJSON(data any) ([]byte, error) {
	return jsoniter.Marshal(data)
}

func UnmarshalJSON(src []byte, data any) error {
	return jsoniter.Unmarshal(src, data)
}
