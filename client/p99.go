package main

import "slices"

func Sort(data []float64) {
	slices.Sort(data)
}

func CalculatePercentile(data []float64, percentile float64) float64 {
	i := (percentile / 100) * float64(len(data))
	return data[int(i)]
}

func Min(data []float64) float64 {
	return data[0]
}

func Max(data []float64) float64 {
	return data[len(data)-1]
}

func Avg(data []float64) float64 {
	var sum float64
	for _, v := range data {
		sum += v
	}
	return sum / float64(len(data))
}
