package main

import (
	"math/rand"
	"time"
	"t_benchmark/ycsb"
	"container/list"
	// "t_txn/aria"
	// "t_txn/bohm"
	"t_txn/caracal"
	// "t_txn/calvin"
	// "t_txn/ody"
	// "t_txn/ador"
	// "t_txn/docc"
	"t_log"
	"os"
	"strconv"
)

func main() {

	rand.Seed(time.Now().UnixNano())
	t_log.Loglevel = t_log.INFO

	theta := 0.7
	scan_len := 8
	if len(os.Args) > 1 {
		theta, _ = strconv.ParseFloat(os.Args[1], 64)
	}
	// write_rates := [...]float64{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9}
	write_rates := [...]float64{0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1}
	for _, write_rate := range(write_rates) {
		bench := ycsb.NewYCSB(0.1, theta, 10, scan_len, write_rate, 0.0)
		// NewYCSB(c float64, a float64, txn_len int, scan_len int, write_rate float64, read_rate float64)
		batch_size := 1000
		l := list.New()
		
		for i := 0 ;i < batch_size; i ++ {
			ops := bench.NewOPS()
			ops.Reset()
			l.PushBack(ops)
		}
		caracal.Run(l, 16)
	}
}