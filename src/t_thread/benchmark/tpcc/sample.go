package main

import (
	"t_log"

	"t_txn/gria"
	"t_txn/aria"
	"t_benchmark/tpcc"
	"t_txn"
	"fmt" 
	"t_thread"
	"t_thread/utils"
)


func Reset(opss [](t_txn.AccessPtr)) {
	for i := 0; i < len(opss); i++ {
		opss[i].Reset()
	}
}

func main() {

	t_log.Loglevel = t_log.PANIC
	// average variance len write_rate
	tpcc_bench := tpcc.NewTPCC(50 , 0.2)
	const t_count = 100
	opss := make([](t_txn.AccessPtr), t_count)
	
	
	
	// for test hang
	// degree := make(chan int, 3)
	

	/* generate txn and reorder(or not) */
	for i := 0; i < t_count; i++ {
		ops := tpcc_bench.NewOPS() // actually read write sequence
		opss[i] = ops
	}

	
	core := 16
	// tpcc
	
	thread_c := []int{1, 2, 3, 5, 8, 12, 16, 24, 32, 64, 128}
	// thread_c := []int{6}
	// thread_c := []int{3}
	// low conflict
	

	fmt.Printf("aria:\n")
	for i := 0; i < len(thread_c); i ++ {
		db := aria.New(2)
		Reset(opss)
		tps := t_thread.RunEC(db, core, thread_c[i], 1, opss, utils.Core_opps())
		fmt.Printf("thread: %v\tktps: %v\t\n", thread_c[i] , tps / 1000)
	}

	fmt.Printf("gria:\n")
	for i := 0; i < len(thread_c); i ++ {
		db := gria.New(2)
		Reset(opss)
		tps := t_thread.RunEC(db, core, thread_c[i], 1, opss, utils.Core_opps())
		fmt.Printf("thread: %v\tktps: %v\t\n", thread_c[i] , tps / 1000)
	}



}