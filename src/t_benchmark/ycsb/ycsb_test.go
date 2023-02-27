package ycsb

/*
go test t_benchmark/ycsb -v
*/


import (
	"testing"
	"fmt"
	// "t_txn"
	// "time"
	// "t_util"
)



func TestWrite(t *testing.T) {
	ycsb := NewYCSB(0.1, 1.1, 10, 0.3, 0.3)
	// NewYCSB(c float64, a float64, txn_len int, write_rate float64, read_rate float64)
	ops := ycsb.NewOPS()

	ops.Reset()

	for true {
		op := ops.Get()
		if op == nil {
			break
		}
		fmt.Printf("[%v, %v, %v]\t", op.Key_start, op.Loop, op.Op_type)
		ops.Next()
	}

}