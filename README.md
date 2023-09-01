# Rotom

## 介绍

​		这里是 Rotom，一个 Go 编写的高性能 Key-Value 内存数据库，基于 RDB + AOF 二进制持久化策略，内置数据类型 String，Map，Set，List，ZSet，BitMap 等，目前只支持在 Golang 中以包引入的方式使用，未来会推出服务端（有可能）。
## 如何使用
在使用之前，请先安装 Rotom 到你的项目中：
```bash
go get github.com/xgzlucario/rotom
```
并安装 gofakeit 库，用于生成一些随机数据：
```bash
go get github.com/brianvoe/gofakeit/v6
```
运行示例程序：
```go
package main

import (
	"fmt"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/xgzlucario/rotom/store"
)

func test() {
	db, err := store.Open(store.DefaultConfig)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Set
	for i := 0; i < 10000; i++ {
		phone := gofakeit.Phone()
		user := gofakeit.Username()

		// Set bytes
		db.Set(phone, []byte(user))

		// Or set with ttl
		db.SetEx(phone, []byte(user), time.Minute)

		// Or set with deadline
		db.SetTx(phone, []byte(user), time.Now().Add(time.Minute).UnixNano())
	}
    
    fmt.Println("now db length is", db.Stat().Len)

	// Get
	key := gofakeit.Phone()
	user, ttl, ok := db.Get(key)
	if ok {
		fmt.Println(string(user), ttl)
	} else {
		fmt.Println("key", key, "is not exist or expired")
	}
}
```
输出：

```
now db length is 10000
key 9241392733 is not exist or expired
```

## 原理

Rotom 是一个**日志型**数据库，将**操作记录**以 **Append** 方式写入日志文件，以**顺序IO**落盘以获得相比随机IO更快的写盘速度。每条操作记录的格式化遵循以下规则：

| OP 操作类型                    | ARGS_NUM 参数个数     | ARGS 参数                    |
| ------------------------------ | --------------------- | ---------------------------- |
| SET、HSET、BITSET，cost 1 byte | 参数个数，cost 1 byte | 数据内容，cost virable bytes |

举个例子，例如操作 **SET xgz 12345**：

|             | OP      | TYPE    | ARGS_NUM           | KEY_VALUE | VAL_VALUE |
| ----------- | ------- | ------- | ------------------ | --------- | --------- |
| **Example** | byte(0) | byte(0) | byte(2)            | 3:xgz     | 5:12345   |
| **Means**   | SET     | String  | 2 args（key, val） | Key       | Val       |

OP byte(0) 表示这是 SET 操作，TYPE byte(0) 表示类型为 String，ARGS_NUM byte(2) 表示参数个数为 2（key, val），KEY 用 `len_of_value:value` 的方式表示，定位 value 的结束点，VAL 同理。以上就表示出了一条 SET 操作。
