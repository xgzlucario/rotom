# structx

Data structures and algorithms implemented using generics.

Currently, structx provides the following types of data structures to support generic types:

- `List`
- `Map`、`SyncMap`
- `LSet (ListSet)`
- `Skiplist`、`ZSet (SortedSet)`
- `Pool`
- `Cache`
- `BitMap`

### BitMap

`bitmap` implement backed by a slice of []uint64, and is `nice wrappered`.

**usage**

```go
bm := structx.NewBitMap(1,2,3)
bm.Add(4) // [1,2,3,4]
bm.Add(1) // [1,2,3.4]
bm.Remove(4) // [1,2,3]
bm.Contains(2) // true

bm.Min() // 1
bm.Max() // 3
bm.Len() // 3

bm1 := structx.NewBitMap(3,4,5)
bm.Union(bm1, true) // [1,2,3,4,5] OR operation and set inplaced
```

**Benchmark**

Benchmarks below were run on a pre-allocated bitmap of **100,000,000** elements.

```
goos: linux
goarch: amd64
pkg: github.com/xgzlucario/structx/test
cpu: AMD Ryzen 7 5800H with Radeon Graphics         
BenchmarkBmAdd-16                  	787935627	         1.515 ns/op	       0 B/op	       0 allocs/op
BenchmarkBmContains-16             	1000000000	         0.3916 ns/op	       0 B/op	       0 allocs/op
BenchmarkBmRemove-16               	1000000000	         0.7613 ns/op	       0 B/op	       0 allocs/op
BenchmarkBmMax-16                  	1000000000	         1.169 ns/op	       0 B/op	       0 allocs/op
BenchmarkBmMin-16                  	1000000000	         0.8804 ns/op	       0 B/op	       0 allocs/op
BenchmarkBmUnion-16                	 2249113	       512.9 ns/op	    2080 B/op	       2 allocs/op
BenchmarkBmUnionInplace-16         	 9901345	       118.2 ns/op	       0 B/op	       0 allocs/op
BenchmarkBmIntersect-16            	 3246958	       370.4 ns/op	    1312 B/op	       2 allocs/op
BenchmarkBmIntersectInplace-16     	11000289	       110.2 ns/op	       0 B/op	       0 allocs/op
BenchmarkBmDifference-16           	 2107741	       606.5 ns/op	    2080 B/op	       2 allocs/op
BenchmarkBmDifferenceInplace-16    	 9769774	       121.4 ns/op	       0 B/op	       0 allocs/op
BenchmarkBmMarshal-16              	      66	  16559485 ns/op	39497262 B/op	       7 allocs/op
PASS
ok  	github.com/xgzlucario/structx/test	32.252s
```

### List

`List` is a data structure wrapping basic type `slice`.  Compare to basic slice type, List is `sequential`, `sortable`, and `nice wrappered`.

#### usage

```go
ls := structx.NewList(1,2,3)
ls.RPush(4) // [1,2,3,4]
ls.LPop() // 1 [2,3,4]
ls.Reverse() // [4,3,2]

ls.Index(1) // 3
ls.Find(4) // 0

ls.RShift() // [2,4,3]
ls.Top(1) // [4,2,3]

ls.Sort(func(i, j int) bool {
	return i<j
}) // [2,3,4]
```

### LSet

`LSet` uses `Map + List` as the storage structure. LSet is Inherited from `List`, where the elements are `sequential` and have `good iterative performance`, as well as `richer api`. When the data volume is small only `list` is used.

#### **usage**

```go
s := structx.NewLSet(1,2,3,4,1) // [1,2,3,4]

s.Add(5) // [1,2,4,5]
s.Remove(3) // [1,2,4]

s.Reverse() // [5,4,2,1]
s.Top(2) // [2,5,4,1]
s.Rpop() // [5,4,1]

s.Range(func(k int) bool {...})

s1 := structx.NewLSet(1,2,3) // [1,2,3]

union := s.Union(s1) // [0,1,2,3]
intersect := s.Intersect(s1) // [1,2]
diff := s.Difference(s1) // [0,3]
```

#### **Benchmark**

Compare with mapset [deckarep/golang-set](https://github.com/deckarep/golang-set).

```
goos: linux
goarch: amd64
pkg: github.com/xgzlucario/structx/test
cpu: AMD Ryzen 7 5800H with Radeon Graphics  
Benchmark_MapSetRange-16          130693	     8991 ns/op	        0 B/op	      0 allocs/op
Benchmark_LSetRange-16            821851	     1415 ns/op	        0 B/op	      0 allocs/op
Benchmark_MapSetRemove-16      318151948	    3.758 ns/op	        0 B/op	      0 allocs/op
Benchmark_LSetRemove-16        364006822	    3.303 ns/op	        0 B/op	      0 allocs/op
Benchmark_MapSetAdd-16         	   21847	    55064 ns/op	    47871 B/op	     68 allocs/op
Benchmark_LSetAdd-16               17355	    68348 ns/op	    73055 B/op	     78 allocs/op
Benchmark_MapSetUnion-16           12676	    94480 ns/op	    47874 B/op	     68 allocs/op
Benchmark_LSetUnion-16             31516	    38181 ns/op	    30181 B/op	     10 allocs/op
Benchmark_MapSetIntersect-16       14566	    82046 ns/op	    47878 B/op	     68 allocs/op
Benchmark_LSetIntersect-16         37855	    31650 ns/op	    30181 B/op	     10 allocs/op
Benchmark_MapSetDiff-16            30876	    38927 ns/op	     8059 B/op	   1002 allocs/op
Benchmark_LSetDiff-16          	   92643	    12866 ns/op	      153 B/op	      4 allocs/op
```

