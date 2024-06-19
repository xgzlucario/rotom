package structx

import "github.com/xgzlucario/quicklist"

// List based on quicklist.
type List struct {
	*quicklist.QuickList
}

func init() {
	quicklist.SetMaxListPackSize(8 * 1024)
}

func NewList() *List {
	return &List{quicklist.New()}
}
