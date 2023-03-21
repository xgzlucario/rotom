package structx

import (
	"fmt"
	"os"
	"time"

	"github.com/xgzlucario/rotom/base"
)

const output = "structx/test"

// GenType
type GenType string

func (g GenType) testNotes() string {
	return fmt.Sprintf(`
	This is the implement of %v, please write test code:
	- Package name is "test", and call the initial method on package "github.com/xgzlucario/rotom/structx".
	- Use wrapped methods whenever possible.
	- Special cases should be taken into consideration.
	- Use strconv.Itoa(int) instead of string(int).
	- Only code without notes.
	`, g)
}

func (g GenType) benchNotes() string {
	return fmt.Sprintf(`
	This is the implement of %v, please write benchmark code:
	- Package name is "test", and call the initial method on package "github.com/xgzlucario/rotom/structx".
	- Use strconv.Itoa(int) instead of string(int).
	- Only one method is tested in each Benchmark test function.
	- Only code without notes.
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

var GenTypes = []GenType{"list", "bitmap", "rbtree", "trie", "zset"}

// InitAI
func InitAI() {
	// mkdir
	if err := os.MkdirAll(output, 0644); err != nil {
		panic(err)
	}

	p := NewPool().WithErrors()
	start := time.Now()

	// gen
	for _, g := range GenTypes {
		g := g

		fmt.Println("init [", g, "] test files...")
		fs, err := os.ReadFile(g.filePath())
		if err != nil {
			panic(err)
		}

		p.Go(func() error {
			// write test file
			content, err := base.Chat(fmt.Sprintf("%s\n%s", fs, g.testNotes()))
			if err != nil {
				return err
			}
			return os.WriteFile(g.testFilePath(), base.S2B(&content), 0644)
		})

		p.Go(func() error {
			// write benchmark file
			content, err := base.Chat(fmt.Sprintf("%s\n%s", fs, g.benchNotes()))
			if err != nil {
				return err
			}
			return os.WriteFile(g.benchFilePath(), base.S2B(&content), 0644)
		})
	}

	if err := p.Wait(); err != nil {
		panic(err)
	}

	fmt.Println("generate code cost:", time.Since(start))
}
