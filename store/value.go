package store

import "github.com/xgzlucario/rotom/base"

// value
type Value struct {
	ok  bool
	key string
	sd  *storeShard

	Raw []byte
	Val any
}

func (v Value) ToInt() (r int, e error) { return getValue(v, r) }

func (v Value) ToInt64() (r int64, e error) { return getValue(v, r) }

func (v Value) ToFloat64() (r float64, e error) { return getValue(v, r) }

func (v Value) ToString() (r string, e error) { return getValue(v, r) }

func (v Value) Scan(val base.Marshaler) error {
	_, err := getValue(v, val)
	return err
}

func (v Value) Error() error {
	if !v.ok {
		return base.ErrKeyNotFound
	}
	return nil
}
