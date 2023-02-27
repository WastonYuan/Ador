package utils

import (
	"time"
)

func op(i int, l int) string {
	var v string
	for j := 0; j < l ; j++ {
		// v = string(i)
		i = i * i
	}
	return v
}

func Core_opps() float64 {
	start := time.Now()
	op_c := 10000000
	op_d := 1
	for i := 0; i < op_c; i++ {
		op(i, op_d)
	}
	elapsed := time.Since(start)
	return float64(op_c) / float64(elapsed.Seconds())
}