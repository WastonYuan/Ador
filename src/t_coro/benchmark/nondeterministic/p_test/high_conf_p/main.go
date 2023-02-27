package main

import (
	"t_log"
	"t_txn/sstpl"
	"t_txn/tpl"
	// "t_txn/sstpl_nw"
	// "t_txn/calvin_nw"
	// "t_txn/bohm"
	// "t_txn/calvin"
	// "t_txn/pwv_nw"
	// "t_txn/pwv"
	// "t_txn/occ_nw"
	// "t_txn/aria"
	// "t_txn/mvcc"
	"t_benchmark"
	"t_txn"
	"fmt"
	"t_coro"
	"t_coro/utils"
	"flag"
)

/*
thread count:
3, 3
and
8, 8
*/


func Reset(opss [](*t_txn.OPS)) {
	for i := 0; i < len(opss); i++ {
		opss[i].Reset()
	}
}

func main() {

	t_log.Loglevel = t_log.PANIC
	average := float64(1000000)
	write_rate := float64(0.5)
	variance := float64(10000)
	t_len := 100
	// average variance len write_rate
	ycsb := t_benchmark.NewYcsb("t", average, variance, t_len, write_rate)
	const t_count = 100
	opss := make([](*(t_txn.OPS)), t_count)
	
	t_flag := flag.Int("t", 1, "specifiy the thread count")
	flag.Parse()

	
	thread_cnt := *t_flag

	

	/* generate txn and reorder(or not) */
	for i := 0; i < t_count; i++ {
		ops := ycsb.NewOPS() // actually read write sequence
		opss[i] = ops
	}

	// core thread p_size
	// for p := 1; p < 16; p ++ {
	// 	fmt.Printf("p_size: %v tps: %v\n", p ,t_coro.Run(db, 8, 2, p, opss))
	// }
	
	core := 16
	// tpcc
	// thread_cnt := 8
	p_v := 100
	// thread_c := []int{3}
	// low conflict
	
	fmt.Printf("tpl:\n")
	for i := 1; i < p_v; i ++ {
		db := tpl.New(2)
		Reset(opss)
		tps, r_cnt, a_cnt := t_coro.Run(db, core, thread_cnt, i, opss, utils.Core_opps())
		fmt.Printf("thread: %v\tp_size: %v\tktps: %v\treset_cnt: %v\tag_cnt: %v\n", thread_cnt , i, tps / 1000, r_cnt, a_cnt)
	}

	fmt.Printf("sstpl:\n")
	for i := 1; i < p_v; i ++ {
		db := sstpl.New(2)
		Reset(opss)
		tps, r_cnt, a_cnt := t_coro.Run(db, core, thread_cnt, i, opss, utils.Core_opps())
		fmt.Printf("thread: %v\tp_size: %v\tktps: %v\treset_cnt: %v\tag_cnt: %v\n", thread_cnt , i, tps / 1000, r_cnt, a_cnt)
	}
	


	// fmt.Println("==================================")
	// for t := 1; t <= 128; t ++ {
	// 	fmt.Printf("thread: %v tps: %v\n", t ,t_coro.Run(db, 10, t, 1, opss))
	// }

	// fmt.Println("================ Test ==================")
	// fmt.Printf("thread: %v tps: %v\n", 1 ,t_coro.Run(db, 10, 1, 1, opss))

}