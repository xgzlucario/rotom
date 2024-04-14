package structx

import "github.com/xgzlucario/quicklist"

// List based on quicklist.
type List struct {
	*quicklist.QuickList
}

func NewList() *List {
	return &List{quicklist.New()}
}

func SetZiplistMaxSize(n int) {
	quicklist.SetEachNodeMaxSize(n)
}
