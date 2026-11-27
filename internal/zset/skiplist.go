package zset

import (
	"math/rand/v2"
)

// skiplist is the redis zskiplist: a skiplist ordered by (score, member)
// whose links record their span, so rank queries are O(log n).
const (
	skiplistMaxLevel = 32
	skiplistP        = 0.25
)

type skiplistLevel struct {
	forward *skiplistNode
	span    int // number of nodes skipped by forward
}

type skiplistNode struct {
	member   string
	score    float64
	backward *skiplistNode
	level    []skiplistLevel
}

type skiplist struct {
	head   *skiplistNode
	tail   *skiplistNode
	length int
	level  int
}

func newSkiplist() *skiplist {
	return &skiplist{
		head:  &skiplistNode{level: make([]skiplistLevel, skiplistMaxLevel)},
		level: 1,
	}
}

func randomLevel() int {
	level := 1
	for level < skiplistMaxLevel && rand.Float64() < skiplistP {
		level++
	}
	return level
}

// less reports whether (score, member) of n sorts before (score, member).
func (n *skiplistNode) less(score float64, member string) bool {
	return n.score < score || (n.score == score && n.member < member)
}

// insert adds a new node. The caller guarantees member is not present.
func (sl *skiplist) insert(score float64, member string) *skiplistNode {
	var update [skiplistMaxLevel]*skiplistNode
	var rank [skiplistMaxLevel]int

	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		if i != sl.level-1 {
			rank[i] = rank[i+1]
		}
		for x.level[i].forward != nil && x.level[i].forward.less(score, member) {
			rank[i] += x.level[i].span
			x = x.level[i].forward
		}
		update[i] = x
	}

	level := randomLevel()
	if level > sl.level {
		for i := sl.level; i < level; i++ {
			rank[i] = 0
			update[i] = sl.head
			update[i].level[i].span = sl.length
		}
		sl.level = level
	}

	x = &skiplistNode{member: member, score: score, level: make([]skiplistLevel, level)}
	for i := 0; i < level; i++ {
		x.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = x
		x.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = rank[0] - rank[i] + 1
	}
	// untouched levels now span one more node
	for i := level; i < sl.level; i++ {
		update[i].level[i].span++
	}

	if update[0] != sl.head {
		x.backward = update[0]
	}
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x
	} else {
		sl.tail = x
	}
	sl.length++
	return x
}

func (sl *skiplist) deleteNode(x *skiplistNode, update *[skiplistMaxLevel]*skiplistNode) {
	for i := 0; i < sl.level; i++ {
		if update[i].level[i].forward == x {
			update[i].level[i].span += x.level[i].span - 1
			update[i].level[i].forward = x.level[i].forward
		} else {
			update[i].level[i].span--
		}
	}
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x.backward
	} else {
		sl.tail = x.backward
	}
	for sl.level > 1 && sl.head.level[sl.level-1].forward == nil {
		sl.level--
	}
	sl.length--
}

// delete removes the node with (score, member). It reports whether it existed.
func (sl *skiplist) delete(score float64, member string) bool {
	var update [skiplistMaxLevel]*skiplistNode
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && x.level[i].forward.less(score, member) {
			x = x.level[i].forward
		}
		update[i] = x
	}
	x = x.level[0].forward
	if x != nil && x.score == score && x.member == member {
		sl.deleteNode(x, &update)
		return true
	}
	return false
}

// rank returns the 1-based rank of (score, member), or 0 if not found.
func (sl *skiplist) rank(score float64, member string) int {
	rank := 0
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.less(score, member) ||
				(x.level[i].forward.score == score && x.level[i].forward.member == member)) {
			rank += x.level[i].span
			x = x.level[i].forward
		}
		if x != sl.head && x.score == score && x.member == member {
			return rank
		}
	}
	return 0
}

// byRank returns the node at 1-based rank, or nil.
func (sl *skiplist) byRank(rank int) *skiplistNode {
	if rank < 1 || rank > sl.length {
		return nil
	}
	traversed := 0
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && traversed+x.level[i].span <= rank {
			traversed += x.level[i].span
			x = x.level[i].forward
		}
		if traversed == rank {
			return x
		}
	}
	return nil
}

func (sl *skiplist) first() *skiplistNode { return sl.head.level[0].forward }
