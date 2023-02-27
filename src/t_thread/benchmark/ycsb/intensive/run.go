package main

import (
	"t_log"

	// "t_txn/gria"
	// "t_txn/aria"
	// "t_txn/calvin"
	// "t_txn/bohm"
	// "t_txn/pwv"
	"t_txn/leaf"
	"t_benchmark"
	"t_txn"
	"fmt"
	"t_thread"
	"t_thread/utils"
	"t_util"
)


func Reset(opss [](t_txn.AccessPtr)) {
	for i := 0; i < len(opss); i++ {
		opss[i].Reset()
	}
}

/*
read intensive: write_rate = 0.1
write intensive: read_rate = 0.9
*/

func main() {

	t_log.Loglevel = t_log.INFO
	a := float64(1000000)
	write_rate := float64(0.9)
	skews := []float64{0.0001, 0.001, 0.01, 0.1, 0.2, 0.4, 0.8, 1}

	t_util.InitConfigurationP()
	t_util.ReadJsonP("configure.json")
	fmt.Println(t_util.Pconf)

	for i := 0; i < len(skews); i++ {

		
		v := float64(1/skews[i])
		t_len := 10
		// average variance len write_rate
		ycsb := t_benchmark.NewYcsb("t", a, v, t_len, write_rate)
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
		
		thread_c := 16
		// fmt.Printf("skew : %v write_rate:%v\n",skews[i], write_rate)
		// fmt.Printf("aria:\t")
		// db := aria.New(2)
		// Reset(opss)
		// tps := t_thread.RunEC(db, core, thread_c, 1, opss, utils.Core_opps())
		// fmt.Printf("thread: %v\tktps: %v\t\n", thread_c , tps / 1000)

		fmt.Printf("leaf:\t")
		db2 := leaf.New(2)
		Reset(opss)
		tps2 := t_thread.RunEC(db2, core, thread_c, 2, opss, utils.Core_opps())
		fmt.Printf("thread: %v\tktps: %v\t\n", thread_c , tps2 / 1000)


		// fmt.Printf("calvin:\t")
		// db3 := calvin.New(2)
		// Reset(opss)
		// tps3 := t_thread.RunPE(db3, core, thread_c, 1, opss, utils.Core_opps())
		// fmt.Printf("thread: %v\tktps: %v\t\n", thread_c , tps3 / 1000)


		// fmt.Printf("bohm:\t")
		// db4 := bohm.New(2)
		// Reset(opss)
		// tps4 := t_thread.RunPE(db4, core, thread_c, 2, opss, utils.Core_opps())
		// fmt.Printf("thread: %v\tktps: %v\t\n", thread_c , tps4 / 1000)

		// fmt.Printf("pwv:\t")
		// db5 := pwv.New(2)
		// Reset(opss)
		// tps5 := t_thread.RunPE(db5, core, thread_c, 2, opss, utils.Core_opps())
		// fmt.Printf("thread: %v\tktps: %v\t\n", thread_c , tps5 / 1000)

	}
}