package tpcc

/*
go test t_benchmark/tpcc -v
*/


import (
	"testing"
	"fmt"
	"sync"
	// "time"
	// "t_util"
)

// func TestSingleThread(t *testing.T) {
// 	tpcc := NewTPCC(2, 0.5)
// 	// rand.Seed(time.Now().Unix())
// 	for i :=0 ;i < 100; i ++ {
// 		ops := tpcc.NewOPS()
// 		for true {
// 			// ops.Reset()
// 			op := ops.Get()
// 			fmt.Println(op)
// 			if op == nil {
// 				break
// 			} else {
// 				ops.Next()
// 			}
// 		}
// 	}
// }


func TestParrel(t *testing.T) {
	tpcc := NewTPCC(2, 0.5)
	// rand.Seed(time.Now().Unix())
	p_count := 1000
	var wg sync.WaitGroup
	wg.Add(p_count)
	for i :=0 ;i < p_count; i ++ {
		go func(txn_id int) {
			defer wg.Done()
			ops := tpcc.NewOPS()
			for true {
				op := ops.Get()
				fmt.Printf("%d: %v\n", txn_id, op)
				if op == nil {
					break
				} else {
					ops.Next()
				}
			}
		}(i)
	}
	wg.Wait()
}