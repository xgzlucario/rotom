[![](https://cdn.nlark.com/yuque/0/2023/svg/23073858/1683826295871-ced6c61b-0cd6-4378-ab58-7240fed72389.svg#clientId=u8bd460fe-a823-4&from=paste&id=u04853372&originHeight=20&originWidth=88&originalType=url&ratio=1.375&rotation=0&showTitle=false&status=done&style=none&taskId=u0d350922-ae97-413a-b256-3b7a728b4ce&title=)](https://goreportcard.com/report/github.com/xgzlucario/rotom) ![](https://img.shields.io/github/languages/code-size/xgzlucario/rotom.svg?style=flat#from=url&id=fHzda&originHeight=20&originWidth=114&originalType=binary&ratio=1.375&rotation=0&showTitle=false&status=done&style=none&title=) [![](https://cdn.nlark.com/yuque/0/2023/svg/23073858/1683826294138-12c7c05a-95ef-47ea-bc38-6f3872ce6fed.svg#clientId=u8bd460fe-a823-4&from=paste&id=uf84068c6&originHeight=20&originWidth=90&originalType=url&ratio=1.375&rotation=0&showTitle=false&status=done&style=none&taskId=u2ef32175-f661-45bc-b5ce-0db1c5f68f1&title=)](https://pkg.go.dev/github.com/xgzlucario/rotom)
这里是 Rotom，一个 Go 编写的高性能 Key-Value 轻量内存数据库，比 Redis 性能快3倍，基于 RDB 和 AOF 混合持久化策略，内置数据类型 String，Map，Set，List，ZSet，BitMap，Trie 等，目前只支持在 Go 中以包引入的方式使用。
## 前言
项目灵感来自于一篇介绍日志型内存数据库的文章。日志型数据库（Log-structured database）是一种特殊类型的数据库，它以日志或追加型方式存储数据，而不是覆盖旧数据。这种类型的数据库通常用于处理大量数据，并能够高效地处理写入操作。
日志型数据库的基础理念是所有的数据库操作都可以视为一系列的日志记录。每次数据变更（插入、更新或删除）都会生成一个新的日志记录，记录着这次变更的内容。这些日志记录会被追加到存储系统的末尾，而不是在旧数据的位置进行更新或删除。
这样做的优点是写入操作的速度很快，因为不需要寻找数据存储的位置，直接追加到末尾即可。此外，日志型数据库也能够提供很好的故障恢复能力，因为所有的数据变更都有日志记录，可以通过重放日志来恢复数据。
## API
待施工。。。
## 使用
在使用之前，请先安装 rotom 到你的项目中。
```bash
go get github.com/xgzlucario/rotom
```
并安装 gofakeit 库，用于生成一些随机数据。
```bash
go get github.com/brianvoe/gofakeit/v6
```
然后运行示例程序：
```go
package main

import (
	"fmt"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/xgzlucario/rotom/store"
)

func main() {
	db, err := store.Open(store.DefaultConfig)
	if err != nil {
		panic(err)
	}
    defer db.Flush()

	// Set
	db.Set("xgz", 23)

	// Get
	age, err := db.Get("xgz").ToInt()
	if err != nil {
		panic(err)
	}
	fmt.Printf("xgz is %d years old.\n", age) // Output: xgz is 23 years old.

	// SetEx
	db.SetEx(gofakeit.Phone(), gofakeit.Uint32(), time.Second*30)

	// SetTx
	ts := time.Date(2075, 2, 19, 0, 0, 0, 0, time.Local)
	db.SetTx(gofakeit.Phone(), gofakeit.Uint32(), ts.UnixNano())

	// Remove
	val, ok := db.Remove("xgz")
	fmt.Println(val, ok) // Output: 23, true

	fmt.Println("db count is", db.Count()) // Output: db count is 2

    // HSet
	db.HSet("hmap", "1", []byte("123"))
	db.HSet("hmap", "2", []byte("234"))

    fmt.Println(db.HGet("hmap", "1")) // Output: 123
	fmt.Println(db.HGet("hmap", "2")) // Output: 234

    // BitSet
    db.BitSet("bit1", 1, true)
	db.BitSet("bit1", 2, true)

	db.BitSet("bit2", 2, true)
	db.BitSet("bit2", 3, true)

    // BitOr
	fmt.Println(db.BitOr("bit1", "bit2", "bit3")) // bit3: [1,2,3]
}

```
## 工作原理
### 重演（rewrite）
本项目灵感来自于一篇介绍数据库的文章，介绍了目前主流内存数据库的持久化方案：如 BTree、B+Tree、LSM Tree、RDB、AOF 等。本项目基于 RDB 和 AOF 混合持久化方案，将 增删改 操作通过日志的方式记录到文件，加载数据时一条一条读取，进行**重演**。例如数据库运行时，有以下操作：
```
SET xgz 22
SET xgz 23
SET abc 123
```
数据库启动时，按照 `SET xgz 22`，`SET xgz 23`，`SET abc 123` 的顺序**重演**，即可完成数据从磁盘到内存的加载。
### 收缩（shrink）
基于 AOF 的存储方式的问题在于，运行过程中日志文件会不断增大，使数据库启动时加载数据变慢。因此需要对其进行**收缩**。具体来说，就是将日志记录进行**删除或合并**，例如有以下操作：
```
SET xgz 22
SET xgz 23
SET abc 123
SET xgz 24
```
数据库中最终应保留 xgz=24，abc=123 这两条数据，而前两条记录为冗余记录，也被称为**脏记录**，在**收缩**时会被删除，下面是收缩后的日志：
```
SET abc 123
SET xgz 24
```
再来看一个例子：
```
SET xgz 22
INCR xgz 1
INCR xgz 2
```
`xgz`经过两次自增操作，最终结果为`25`，因此它也等价为`SET xgz 25`。经过**收缩**后，这三条操作记录会被**合并**，合并后的日志即为：
```
SET xgz 25
```
### 哈希表（Hashmap）
#### 实现方法
Rotom 的数据存储核心是一个 HashMap，基于 开放地址法 和 [Robin hood hashing](https://en.wikipedia.org/wiki/Hash_table#Robin_Hood_hashing) 冲突算法。简单来说，发生冲突时具有较高 DIB 的键值对会“**抢夺**”较低 DIB的键值对的位置，然后较低 DIB 的键值对会**向后移动**寻找新的位置。这样做的目的是尽量**保持 DIB 较低**，从而使得哈希映射的查找性能更好。
使用该结构的优点在于，数据全部存储在一个数组中，**随机探测**性能很高，且不需要额外数据结构。只需要一个哈希表就能完成**数据存储**+**过期淘汰策略**。
#### 淘汰策略
在 Rotom 中，数据库引擎为 Cache 结构。
```go
type cacheItem[V any] struct {
    T int64
    V V
}

type Cache[V any] struct {
	// current timestamp
	ts int64

	// based on Hashmap
	data Map[string, *cacheItem[V]]

	// Reuse object to reduce GC stress
	pool sync.Pool

	mu sync.RWMutex
}
```
## 工作流程
待施工。。。
