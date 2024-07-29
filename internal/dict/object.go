package dict

import (
	"fmt"
	"time"

	"github.com/xgzlucario/rotom/internal/hash"
	"github.com/xgzlucario/rotom/internal/list"
	"github.com/xgzlucario/rotom/internal/zset"
)

// Type defines all rotom data types.
type Type byte

const (
	TypeString Type = iota + 1
	TypeInteger
	TypeMap
	TypeZipMap
	TypeSet
	TypeZipSet
	TypeList
	TypeZSet
)

// Object is the basic element for storing in dict.
type Object struct {
	typ         Type
	hasTTL      bool
	lastAccessd uint32
	data        any
}

func (o *Object) Type() Type { return o.typ }

func (o *Object) Data() any { return o.data }

func (o *Object) SetData(data any) {
	o.typ = typeOfData(data)
	o.data = data
}

func nsec2duration(nsec int64) (second int) {
	return int(nsec-_nsec.Load()) / int(time.Second)
}

func typeOfData(data any) Type {
	switch data.(type) {
	case []byte:
		return TypeString
	case int:
		return TypeInteger
	case *hash.Map:
		return TypeMap
	case *hash.ZipMap:
		return TypeZipMap
	case *hash.Set:
		return TypeSet
	case *hash.ZipSet:
		return TypeZipSet
	case *list.QuickList:
		return TypeList
	case *zset.ZSet:
		return TypeZSet
	default:
		panic(fmt.Sprintf("unknown type: %T", data))
	}
}
