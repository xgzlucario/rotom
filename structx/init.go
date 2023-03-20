package structx

import (
	"fmt"
	"os"

	"github.com/xgzlucario/rotom/base"
)

const output = "structx/test"

// GenType
type GenType string

func (g GenType) genTest() string {
	return fmt.Sprintf(`
	This is the implementation of %v, please help me write test code:
	1. Pakage name is "test", and call the initial method on package "structx".
	2. Use wrapperd methods preffered.
	3. Special cases should be taken into consideration.
	4. Only code without notes.
	`, g)
}

func (g GenType) genBench() string {
	return fmt.Sprintf(`
	This is the implementation of %v, please help me write benchmark code:
	1. Pakage name is "test", and call the initial method on package "structx".
	2. Use wrapperd methods preffered.
	3. Only code without notes.
	`, g)
}

func (g GenType) genFilePath() string {
	return fmt.Sprintf("structx/%s.go", g)
}

func (g GenType) genTestFilePath() string {
	return fmt.Sprintf("%s/%s_test.go", output, g)
}

func (g GenType) genBenchFilePath() string {
	return fmt.Sprintf("%s/%s_bench_test.go", output, g)
}

var GenTypes = []GenType{"list", "set", "bitmap", "rbtree", "trie", "array2d", "zset"}

// InitAI
func InitAI() {
	// mkdir
	if err := os.MkdirAll(output, 0644); err != nil {
		panic(err)
	}

	for _, g := range GenTypes {
		fmt.Println("init [", g, "] test file...")
		fs, err := os.ReadFile(g.genFilePath())
		if err != nil {
			panic(err)
		}

		// write test file
		content, err := base.Chat(fmt.Sprintf("%s\n%s", fs, g.genTest()))
		if err != nil {
			fmt.Println(err)

		} else {
			if err := os.WriteFile(g.genTestFilePath(), base.S2B(&content), 0644); err != nil {
				panic(err)
			}
		}

		// write benchmark file
		content, err = base.Chat(fmt.Sprintf("%s\n%s", fs, g.genBench()))
		if err != nil {
			fmt.Println(err)

		} else {
			if err := os.WriteFile(g.genBenchFilePath(), base.S2B(&content), 0644); err != nil {
				panic(err)
			}
		}
	}
}
