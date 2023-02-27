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
	"fmt"
	"time"
	"os"
) 

func main() {
	rand.Seed(time.Now().UnixNano())
	t_log.Loglevel = t_log.INFO
	bench := tpce.NewTPCC(32)
	// NewYCSB(c float64, a float64, txn_len int, write_rate float64, read_rate float64)

	batch_size := 1000
	
	// fmt.Println(len(os.Args), os.Args)
	thread_cnt := 8

	if len(os.Args) > 1 {
		thread_cnt, _ = strconv.Atoi(os.Args[1])
	}
	queue_sizes := [...]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,16, 32, 64, 128, 256, 512, 1024}
	// fmt.Printf("thread_cnt:%v\n", thread_cnt)
	for _, qs := range(queue_sizes) {
		var ops t_txn.AccessPtr
		ody.QueueSize = qs
		fmt.Printf("QueueSize: %v\n", ody.QueueSize)
		l := list.New()
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
	
}