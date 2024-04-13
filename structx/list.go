package structx

import "github.com/xgzlucario/quicklist"

type List struct {
	*quicklist.QuickList
}

func NewList() *List {
	return &List{quicklist.New()}
}

func SetZiplistMaxSize(n int) {
	quicklist.SetEachNodeMaxSize(n)
}
