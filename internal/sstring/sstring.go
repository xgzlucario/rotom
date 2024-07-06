// package sstring is shared strings.

package sstring

import (
	"strings"

	"github.com/cockroachdb/swiss"
)

var nums = swiss.New[string, int](8)
var strs []string

// Load a shared string from its number.
func Load(num int) (str string) {
	if num >= 0 && num < len(strs) {
		str = strs[num]
		return str
	}
	panic("string not found")
}

// Store a shared string.
func Store(str string) int {
	num, ok := nums.Get(str)
	if !ok {
		str = strings.Clone(str)
		num = len(strs)
		strs = append(strs, str)
		nums.Put(str, num)
	}
	return num
}

// Len returns the number of shared strings.
func Len() int {
	return len(strs)
}
