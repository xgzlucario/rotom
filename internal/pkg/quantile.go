package pkg

import (
	"fmt"
	"slices"
	"time"
)

type Quantile struct {
	f []float64
}

func NewQuantile(size int) *Quantile {
	return &Quantile{f: make([]float64, 0, size)}
}

func (q *Quantile) Add(v float64) {
	q.f = append(q.f, v)
}

func (q *Quantile) quantile(p float64) float64 {
	r := q.f[int(float64(len(q.f))*p)]
	return r
}

func (q *Quantile) Print() {
	slices.Sort(q.f)
	fmt.Printf("90th: %v\n", time.Duration(q.quantile(0.9)))
	fmt.Printf("99th: %v\n", time.Duration(q.quantile(0.99)))
	fmt.Printf("999th: %v\n", time.Duration(q.quantile(0.999)))
	fmt.Printf("max: %v\n", time.Duration(q.f[len(q.f)-1]))
}
