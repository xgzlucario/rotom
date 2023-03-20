package structx

import (
	"fmt"
	"os"

	"github.com/xgzlucario/rotom/base"
)

const output = "structx/test"

// GenType
type GenType string

func (g GenType) testNotes() string {
	return fmt.Sprintf(`
	This is the implementation of %v, please write test code:
	1. Pakage name is "test", and call the initial method on package "structx".
	2. Use wrapperd methods preffered.
	3. Special cases should be taken into consideration.
	4. Only code without notes.
	`, g)
}

func (g GenType) benchNotes() string {
	return fmt.Sprintf(`
	This is the implementation of %v, please write benchmark code:
	1. Pakage name is "test", and call the initial method on package "structx".
	2. Use wrapperd methods preffered.
	3. Only code without notes.
	`, g)
}

func (g GenType) filePath() string {
	return fmt.Sprintf("structx/%s.go", g)
}

func (g GenType) testFilePath() string {
	return fmt.Sprintf("%s/%s_test.go", output, g)
}

func (g GenType) benchFilePath() string {
	return fmt.Sprintf("%s/%s_bench_test.go", output, g)
}

var GenTypes = []GenType{"list", "bitmap", "rbtree", "trie", "array2d", "zset"}

// InitAI
func InitAI() {
	// mkdir
	if err := os.MkdirAll(output, 0644); err != nil {
		panic(err)
	}

	for _, g := range GenTypes {
		fmt.Println("init [", g, "] test files...")
		fs, err := os.ReadFile(g.filePath())
		if err != nil {
			panic(err)
		}

		// write test file
		content, err := base.Chat(fmt.Sprintf("%s\n%s", fs, g.testNotes()))
		if err != nil {
			fmt.Println(err)

		} else {
			if err := os.WriteFile(g.testFilePath(), base.S2B(&content), 0644); err != nil {
				panic(err)
			}
		}

		// write benchmark file
		content, err = base.Chat(fmt.Sprintf("%s\n%s", fs, g.benchNotes()))
		if err != nil {
			fmt.Println(err)

		} else {
			if err := os.WriteFile(g.benchFilePath(), base.S2B(&content), 0644); err != nil {
				panic(err)
			}
		}
	}
}
