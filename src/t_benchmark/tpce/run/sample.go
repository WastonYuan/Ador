package main

import (
	"math/rand"
	"t_benchmark/tpce"
	"container/list"
	"t_txn"
	"t_txn/ody"
	// "t_txn/docc"
	// "t_txn/aria"
	// "t_txn/ariax"
	// "t_txn/bohm"
	// "t_txn/caracal"
	// "t_txn/calvin"
	"t_log"
	"strconv"
	// "fmt"
	"time"
	"os"
) 

func main() {
	rand.Seed(time.Now().UnixNano())
	t_log.Loglevel = t_log.INFO
	bench := tpce.NewTPCC(1)
	// NewYCSB(c float64, a float64, txn_len int, write_rate float64, read_rate float64)

	batch_size := 1000
	l := list.New()
	// fmt.Println(len(os.Args), os.Args)
	thread_cnt := 8
	
	if len(os.Args) > 1 {
		thread_cnt, _ = strconv.Atoi(os.Args[1])
	}
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
	ody.Run(l, thread_cnt)
}