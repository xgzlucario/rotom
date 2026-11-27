package db

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHashtable(t *testing.T) {
	ast := assert.New(t)
	h := newHashtable()
	ref := map[string]*entry{}
	for i := range 50000 {
		key := fmt.Sprint(rand.IntN(20000))
		// bias towards inserts first, then deletes, to exercise grow and shrink.
		insert := rand.IntN(100) < 70
		if i > 25000 {
			insert = rand.IntN(100) < 30
		}
		if insert {
			if _, ok := ref[key]; !ok {
				e := &entry{key: key}
				h.Insert(e)
				ref[key] = e
			}
		} else {
			e, ok := ref[key]
			if ok {
				ast.Same(e, h.Delete(key))
			} else {
				ast.Nil(h.Delete(key))
			}
			delete(ref, key)
		}
		if i%1000 == 0 {
			ast.Equal(len(ref), h.Len())
		}
	}
	for k, e := range ref {
		ast.Same(e, h.Get(k))
	}
	ast.Nil(h.Get("missing"))
	n := 0
	for range h.All() {
		n++
	}
	ast.Equal(len(ref), n)
}

// TestHashtableScanGuarantee checks the SCAN contract: keys present for the
// whole iteration are returned at least once, even when the table grows,
// shrinks and rehashes between calls.
func TestHashtableScanGuarantee(t *testing.T) {
	for _, mode := range []string{"grow", "shrink"} {
		t.Run(mode, func(t *testing.T) {
			h := newHashtable()
			stable := map[string]bool{}
			for i := range 1000 {
				key := fmt.Sprint("stable", i)
				h.Insert(&entry{key: key})
				stable[key] = true
			}
			var churn []string
			if mode == "shrink" {
				for i := range 20000 {
					key := fmt.Sprint("churn", i)
					h.Insert(&entry{key: key})
					churn = append(churn, key)
				}
			}

			seen := map[string]bool{}
			cursor, step := uint64(0), 0
			for {
				cursor = h.Scan(cursor, func(e *entry) { seen[e.key] = true })
				step++
				// mutate between calls: forces resizes and incremental rehash.
				if mode == "grow" {
					// fewer inserts than buckets visited, or the scan never ends
					// (same as redis with a tiny COUNT under heavy writes).
					if step%2 == 0 {
						h.Insert(&entry{key: fmt.Sprint("new", step)})
					}
				} else if len(churn) > 0 {
					n := min(len(churn), 50)
					for _, k := range churn[:n] {
						h.Delete(k)
					}
					churn = churn[n:]
				}
				if cursor == 0 {
					break
				}
				if step > 200_000 {
					t.Fatal("scan did not terminate")
				}
			}
			for key := range stable {
				if !seen[key] {
					t.Fatalf("key %s present during the whole scan was not returned", key)
				}
			}
		})
	}
}
