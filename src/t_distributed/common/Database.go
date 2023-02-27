package common

import (
	"t_log"

	"t_txn/gria"
	"t_txn/aria"
	"t_txn/calvin"
	"t_txn/bohm"
	"t_txn/pwv"
	// "t_benchmark"
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

func Run(opss [](t_txn.AccessPtr)) {

		
	t_log.Loglevel = t_log.INFO
	// core thread p_size
	// for p := 1; p < 16; p ++ { 
	// 	fmt.Printf("p_size: %v tps: %v\n", p ,t_coro.Run(db, 8, 2, p, opss))
	// }
	
	core := 16
	// tpcc
	
	thread_c := t_util.Pconf.Thread
	
	if t_util.Pconf.Server == "Aria" {
		fmt.Printf("aria:\t")
		db := aria.New(2)
		Reset(opss)
		t_thread.RunEC(db, core, thread_c, 1, opss, utils.Core_opps())
		// fmt.Printf("thread: %v\tktps: %v\t\n", thread_c , tps / 1000)
	} else if t_util.Pconf.Server == "Calvin" {
		fmt.Printf("calvin:\t")
		db3 := calvin.New(2)
		Reset(opss)
		t_thread.RunPE(db3, core, thread_c, 1, opss, utils.Core_opps())
		// fmt.Printf("thread: %v\tktps: %v\t\n", thread_c , tps3 / 1000)
	} else if t_util.Pconf.Server == "BOHM" { 
		fmt.Printf("bohm:\t")
		db4 := bohm.New(2)
		Reset(opss)
		t_thread.RunPE(db4, core, thread_c, 2, opss, utils.Core_opps())
		// fmt.Printf("thread: %v\tktps: %v\t\n", thread_c , tps4 / 1000)
	} else if t_util.Pconf.Server == "Caracal" {
		fmt.Printf("Caracal:\t")
		db5 := pwv.New(2)
		Reset(opss)
		t_thread.RunPE(db5, core, thread_c, 2, opss, utils.Core_opps())
		// fmt.Printf("thread: %v\tps: %v\t\n", thread_c , tps5 / 1000)
	} else {
		fmt.Printf("gria:\t")
		db2 := gria.New(2)
		Reset(opss)
		t_thread.RunEC(db2, core, thread_c, 2, opss, utils.Core_opps())
		// fmt.Printf("thread: %v\ttps: %v\t\n", thread_c , tps2 / 1000)
	}

}