package main

import (
	"math/rand"
	"t_benchmark/tpce"
	"container/list"
	"t_txn"
	// "t_txn/ody"
	// "t_txn/ody_less"
	// "t_txn/docc"
	// "t_txn/ador"
	// "t_txn/ador4docc"
	// "t_txn/ador_we"
	// "t_txn/docc"
	"t_txn/aria"
	// "t_txn/ariax"
	// "t_txn/bohm"
	// "t_txn/caracal"
	// "t_txn/calvin"
	"t_log"
	// "fmt"
	"time"
) 

func main() {
	rand.Seed(time.Now().UnixNano())
	t_log.Loglevel = t_log.INFO
	bench := tpce.NewTPCC(8)
	// NewYCSB(c float64, a float64, txn_len int, write_rate float64, read_rate float64)

	batch_size := 1000
	
	// fmt.Println(len(os.Args), os.Args)
	thd_cnts := [...]int{1, 2, 4, 8, 12, 16, 24, 32}
	for _, thd_cnt := range(thd_cnts) {
		l := list.New()
		// fmt.Printf("thread_cnt:%v\n", thread_cnt)
		var ops t_txn.AccessPtr
		for i := 0 ;i < batch_size; i ++ {
			if rand.Float64() < 0.5 {
				ops = bench.NewOPS(tpce.NEW_ORDER)
			} else {
				ops = bench.NewOPS(tpce.PAYMENT)
			}
			ops.Reset()
			l.PushBack(ops)
		}
		aria.Run(l, thd_cnt)
	}
}