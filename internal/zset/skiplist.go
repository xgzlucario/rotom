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
	for level < skiplistMaxLevel && rand.Uint32() < uint32(skiplistP*(1<<32)) {
		level++
	}
	return level
}

// newNode allocates a node and its levels in a single object for the common
// small levels (~98% of nodes have level <= 3), like redis' flexible array
// member. This halves allocations and keeps a node's levels next to its key,
// so a lookup touches one cache line less per visited node.
func newNode(level int, score float64, member string) *skiplistNode {
	switch level {
	case 1:
		n := &struct {
			skiplistNode
			levels [1]skiplistLevel
		}{}
		n.level = n.levels[:]
		n.score, n.member = score, member
		return &n.skiplistNode
	case 2:
		n := &struct {
			skiplistNode
			levels [2]skiplistLevel
		}{}
		n.level = n.levels[:]
		n.score, n.member = score, member
		return &n.skiplistNode
	case 3:
		n := &struct {
			skiplistNode
			levels [3]skiplistLevel
		}{}
		n.level = n.levels[:]
		n.score, n.member = score, member
		return &n.skiplistNode
	}
	return &skiplistNode{member: member, score: score, level: make([]skiplistLevel, level)}
}

// less reports whether n sorts before (score, member).
func (n *skiplistNode) less(score float64, member string) bool {
	return n.score < score || (n.score == score && n.member < member)
}

// findUpdate walks to the last node before (score, member) on every level,
// recording it in update and its rank in rank (rank may be nil).
func (sl *skiplist) findUpdate(score float64, member string, update *[skiplistMaxLevel]*skiplistNode, rank *[skiplistMaxLevel]int) *skiplistNode {
	x := sl.head
	traversed := 0
	for i := sl.level - 1; i >= 0; i-- {
		for {
			next := x.level[i].forward
			if next == nil || !next.less(score, member) {
				break
			}
			traversed += x.level[i].span
			x = next
		}
		update[i] = x
		if rank != nil {
			rank[i] = traversed
		}
	}
	return x
}

// insert adds a new node. The caller guarantees member is not present.
func (sl *skiplist) insert(score float64, member string) *skiplistNode {
	x := newNode(randomLevel(), score, member)
	sl.link(x)
	return x
}

// link inserts the detached node x at its position by (score, member).
func (sl *skiplist) link(x *skiplistNode) {
	var update [skiplistMaxLevel]*skiplistNode
	var rank [skiplistMaxLevel]int
	sl.findUpdate(x.score, x.member, &update, &rank)

	level := len(x.level)
	if level > sl.level {
		for i := sl.level; i < level; i++ {
			rank[i] = 0
			update[i] = sl.head
			update[i].level[i].span = sl.length
		}
		sl.level = level
	}

	for i := range level {
		x.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = x
		x.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = rank[0] - rank[i] + 1
	}
	// untouched levels now span one more node
	for i := level; i < sl.level; i++ {
		update[i].level[i].span++
	}

	x.backward = nil
	if update[0] != sl.head {
		x.backward = update[0]
	}
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x
	} else {
		sl.tail = x
	}
	sl.length++
}

// unlink removes x given its predecessors on every level.
func (sl *skiplist) unlink(x *skiplistNode, update *[skiplistMaxLevel]*skiplistNode) {
	for i := range sl.level {
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

// find returns the node with (score, member) and its predecessors, or nil.
func (sl *skiplist) find(score float64, member string, update *[skiplistMaxLevel]*skiplistNode) *skiplistNode {
	x := sl.findUpdate(score, member, update, nil).level[0].forward
	if x != nil && x.score == score && x.member == member {
		return x
	}
	return nil
}

// delete removes the node with (score, member). It reports whether it existed.
func (sl *skiplist) delete(score float64, member string) bool {
	var update [skiplistMaxLevel]*skiplistNode
	x := sl.find(score, member, &update)
	if x == nil {
		return false
	}
	sl.unlink(x, &update)
	return true
}

// updateScore changes the score of the existing (oldScore, member) node,
// like redis' zslUpdateScore: if the node still sorts between its neighbours
// it is updated in place, otherwise it is moved, reusing the node so that no
// allocation happens.
func (sl *skiplist) updateScore(oldScore float64, member string, newScore float64) {
	var update [skiplistMaxLevel]*skiplistNode
	x := sl.find(oldScore, member, &update)

	prev, next := x.backward, x.level[0].forward
	if (prev == nil || prev.less(newScore, member)) &&
		(next == nil || !next.less(newScore, member)) {
		x.score = newScore
		return
	}
	sl.unlink(x, &update)
	x.score = newScore
	sl.link(x)
}

// rank returns the 1-based rank of (score, member), or 0 if not found.
func (sl *skiplist) rank(score float64, member string) int {
	rank := 0
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for {
			next := x.level[i].forward
			if next == nil || !(next.less(score, member) || (next.score == score && next.member == member)) {
				break
			}
			rank += x.level[i].span
			x = next
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
