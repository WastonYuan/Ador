package main

import (
	"t_benchmark/ycsb"
	"container/list"
	// "t_txn/aria"
	// "t_txn/bohm"
	// "t_txn/caracal"
	"t_txn/ody_less"
	// "t_txn/calvin"
	// "t_txn/ody"
	// "t_txn/docc"
	"t_log"
	"math/rand"
	"time"
	"os"
	"strconv"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	t_log.Loglevel = t_log.INFO

	theta := 0.9
	scan_len := 16
	if len(os.Args) > 1 {
		theta, _ = strconv.ParseFloat(os.Args[1], 64)
	}

	bench := ycsb.NewYCSB(0.1, theta, 100, scan_len, 0.2, 0.8)
	// NewYCSB(c float64, a float64, txn_len int, scan_len int, write_rate float64, read_rate float64)
	
	batch_size := 1000
	l := list.New()
	
	for i := 0 ;i < batch_size; i ++ {
		ops := bench.NewOPS()
		ops.Reset()
		l.PushBack(ops)
	}
	ody_less.Run(l, 2)
}