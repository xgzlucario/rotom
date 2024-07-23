package dict

// Type defines all rotom data types.
type Type byte

const (
	TypeString Type = iota + 1
	TypeInt
	TypeMap
	TypeZipMap
	TypeZipMapC // compressed zipmap
	TypeSet
	TypeZipSet
	TypeZipSetC // compressed zipset
	TypeList
	TypeZSet
)

type Compressor interface {
	Compress()
	Decompress()
}

// Object is the basic element for storing in dict.
type Object struct {
	typ         Type
	hasTTL      bool
	lastAccessd uint32
	data        any
}

func (o *Object) Type() Type { return o.typ }

func (o *Object) Data() any { return o.data }

func (o *Object) SetData(data any) { o.data = data }
