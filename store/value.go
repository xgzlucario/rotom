package store

import (
	"time"

	"github.com/xgzlucario/rotom/base"
)

// value
type Value struct {
	key string
	sd  *storeShard
	raw []byte
	val any
}

func (v Value) ToInt() (r int, e error) { return getValue(v, r) }

func (v Value) ToInt64() (r int64, e error) { return getValue(v, r) }

func (v Value) ToUint() (r uint, e error) { return getValue(v, r) }

func (v Value) ToUint64() (r uint64, e error) { return getValue(v, r) }

func (v Value) ToFloat64() (r float64, e error) { return getValue(v, r) }

func (v Value) ToString() (r string, e error) { return getValue(v, r) }

func (v Value) ToIntSlice() (r []int, e error) { return getValue(v, r) }

func (v Value) ToStringSlice() (r []string, e error) { return getValue(v, r) }

func (v Value) ToTime() (r time.Time, e error) { return getValue(v, r) }

func (v Value) Scan(val any) error {
	_, err := getValue(v, val)
	return err
}

// getValue
func getValue[T any](v Value, vptr T) (T, error) {
	if v.raw != nil {
		if err := v.sd.Decode(v.raw, &vptr); err != nil {
			return vptr, err
		}

		v.sd.Set(v.key, vptr)
		return vptr, nil
	}

	if tmp, ok := v.val.(T); ok {
		return tmp, nil

	} else if v.key == "" {
		return vptr, base.ErrKeyNotFound

	} else {
		return vptr, base.ErrWrongType
	}
}
