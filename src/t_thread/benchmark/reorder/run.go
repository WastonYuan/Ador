package main

import (
	"t_log"

	"t_txn/gria"
	// "t_txn/aria"
	// "t_txn/calvin"
	// "t_txn/bohm"
	// "t_txn/pwv"
	"t_benchmark"
	"t_txn"
	"fmt"
	"t_thread"
	"t_thread/utils"
	"container/list"
)


func Reset(opss [](t_txn.AccessPtr)) {
	for i := 0; i < len(opss); i++ {
		opss[i].Reset()
	}
}

func main() {

	t_log.Loglevel = t_log.INFO
	average := float64(1000000)
	write_rate := float64(0.4)
	t_len := 100
	// average variance len write_rate
	
	const t_count = 100
	
	skews := []float64{0.000001, 0.00001, 0.0001, 0.001, 0.01, 1}
	
	// for test hang
	// degree := make(chan int, 3)
	
	opss_list := list.New()

	/* generate txn and reorder(or not) */
	for j := 0; j < len(skews); j++ {
		skew := skews[j]
		ycsb := t_benchmark.NewYcsb("t", average, 1/skew, t_len, write_rate)
		opss := make([](t_txn.AccessPtr), t_count)
		for i := 0; i < t_count; i++ {
			ops := ycsb.NewOPS() // actually read write sequence
			opss[i] = ops
		}
		opss_list.PushBack(&opss)

	}

	// core thread p_size
	// for p := 1; p < 16; p ++ { 
	// 	fmt.Printf("p_size: %v tps: %v\n", p ,t_coro.Run(db, 8, 2, p, opss))
	// }
	
	core := 16
	// tpcc
	
	thread_c := 16


	
	fmt.Printf("gria:\n")
	for ele := opss_list.Front(); ele != nil; ele = ele.Next() {
		opss := ele.Value.(*([](t_txn.AccessPtr)))
		db := gria.New(2)
		gria.Reorder = false
		Reset(*opss)
		tps := t_thread.RunEC(db, core, thread_c, 1, *opss, utils.Core_opps(), false)
		fmt.Printf("thread: %v\tktps: %v\t\n", thread_c , tps / 1000)
	}

	fmt.Printf("gria_reoder:\n")
	for ele := opss_list.Front(); ele != nil; ele = ele.Next() {
		opss := ele.Value.(*([](t_txn.AccessPtr)))
		db_r := gria.New(2)
		gria.Reorder = true
		Reset(*opss)
		tps_r := t_thread.RunEC(db_r, core, thread_c, 1, *opss, utils.Core_opps(), false)
		fmt.Printf("thread: %v\tktps: %v\t\n", thread_c , tps_r / 1000)
	}

	// fmt.Println("==================================")
	// for t := 1; t <= 128; t ++ {
	// 	fmt.Printf("thread: %v tps: %v\n", t ,t_coro.Run(db, 10, t, 1, opss))
	// }

	// fmt.Println("================ Test ==================")
	// fmt.Printf("thread: %v tps: %v\n", 1 ,t_coro.Run(db, 10, 1, 1, opss))

}