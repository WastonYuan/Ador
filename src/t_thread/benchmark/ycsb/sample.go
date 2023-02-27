package main

import (
	"t_log"

	"t_txn/gria"
	"t_txn/aria"
	"t_txn/calvin"
	"t_txn/bohm"
	"t_txn/pwv"
	"t_benchmark"
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
	average := float64(1000000)
	write_rate := float64(0.2)
	variance := float64(5000)
	t_len := 100
	// average variance len write_rate
	ycsb := t_benchmark.NewYcsb("t", average, variance, t_len, write_rate)
	const t_count = 100
	opss := make([](t_txn.AccessPtr), t_count)
	
	
	
	// for test hang
	// degree := make(chan int, 3)
	

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
	
	thread_c := []int{1, 2, 3, 5, 8, 12, 16, 24, 32, 64, 128}
	// thread_c := []int{6}
	// thread_c := []int{3}
	// low conflict
	
	// fmt.Printf("gria:\n")
	// for i := 0; i < len(thread_c); i ++ {
	// 	db := gria.New(2)
	// 	Reset(opss)
	// 	tps := t_thread.Run(db, core, thread_c[i], 1, opss, utils.Core_opps())
	// 	fmt.Printf("thread: %v\tktps: %v\t\n", thread_c[i] , tps / 1000)
	// }

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


	fmt.Printf("calvin:\n")
	for i := 0; i < len(thread_c); i ++ {
		db := calvin.New(2)
		Reset(opss)
		tps := t_thread.RunPE(db, core, thread_c[i], 1, opss, utils.Core_opps())
		fmt.Printf("thread: %v\tktps: %v\t\n", thread_c[i] , tps / 1000)
	}


	fmt.Printf("bohm:\n")
	for i := 0; i < len(thread_c); i ++ {
		db := bohm.New(2)
		Reset(opss)
		tps := t_thread.RunPE(db, core, thread_c[i], 1, opss, utils.Core_opps())
		fmt.Printf("thread: %v\tktps: %v\t\n", thread_c[i] , tps / 1000)
	}

	fmt.Printf("pwv:\n")
	for i := 0; i < len(thread_c); i ++ {
		db := pwv.New(2)
		Reset(opss)
		tps := t_thread.RunPE(db, core, thread_c[i], 1, opss, utils.Core_opps())
		fmt.Printf("thread: %v\tktps: %v\t\n", thread_c[i] , tps / 1000)
	}


	// fmt.Println("==================================")
	// for t := 1; t <= 128; t ++ {
	// 	fmt.Printf("thread: %v tps: %v\n", t ,t_coro.Run(db, 10, t, 1, opss))
	// }

	// fmt.Println("================ Test ==================")
	// fmt.Printf("thread: %v tps: %v\n", 1 ,t_coro.Run(db, 10, 1, 1, opss))

}