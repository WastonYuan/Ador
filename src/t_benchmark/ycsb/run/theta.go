package main

import (
	"math/rand"
	"time"
	"t_benchmark/ycsb"
	"container/list"
	// "t_txn/aria"
	// "t_txn/bohm"
	// "t_txn/caracal"
	// "t_txn/calvin"
	// "t_txn/ody"
	// "t_txn/docc"
	"t_txn/ador"
	// "t_txn/ador_we"
	// "t_txn/ador4docc"
	"t_log"
	// "os"
	// "strconv"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	t_log.Loglevel = t_log.INFO

	thetas := [...]float64{0.1, 0.3, 0.5, 0.7, 0.9, 1.1, 1.3, 1.5}
	scan_len := 4

	
	// NewYCSB(c float64, a float64, txn_len int, scan_len int, write_rate float64, read_rate float64)
	
	batch_size := 1000

	for _, theta := range(thetas) {
		bench := ycsb.NewYCSB(0.1, theta, 10, scan_len, 0.9, 0.1)
		l := list.New()
		for i := 0 ;i < batch_size; i ++ {
			ops := bench.NewOPS()
			ops.Reset()
			l.PushBack(ops)
		}
		ador.Run(l, 16)
	}
}