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
	"t_util"
)


// high intention: 16
// low intention 256

func Reset(opss [](t_txn.AccessPtr)) {
	for i := 0; i < len(opss); i++ {
		opss[i].Reset()
	}
}

func main() {

	t_log.Loglevel = t_log.INFO
	Warehouse := 16
	// average variance len write_rate
	tpcc_bench := tpcc.NewTPCC(Warehouse , 0.5)
	for i := 100 ; i <= 1000; i = i + 100 {
		fmt.Println("epoch_size:%d\n", i)
		t_count := i // epoch size
		opss := make([](t_txn.AccessPtr), t_count)
		
		
		
		// for test hang
		// degree := make(chan int, 3)
		

		/* generate txn and reorder(or not) */
		for i := 0; i < t_count; i++ {
			ops := tpcc_bench.NewOPS() // actually read write sequence

			opss[i] = ops
		}

		t_util.InitConfigurationP()
    	t_util.ReadJsonP("configure.json")
    	fmt.Println(t_util.Pconf)

		core := 16
		// tpcc

		thread_c := 32
		// thread_c := []int{6}
		// thread_c := []int{3}
		// low conflict



		fmt.Println("Warehouse: ", Warehouse)
		fmt.Printf("aria:\n")
		db2 := aria.New(2)
		Reset(opss)
		tps2 := t_thread.RunEC(db2, core, thread_c, 1, opss, utils.Core_opps())
		fmt.Printf("thread: %v\ttps: %v\t\n", thread_c , tps2 / 1000000)

		fmt.Println("Warehouse: ", Warehouse)
		fmt.Printf("gria:\n")
		db := gria.New(2)
		Reset(opss)
		tps := t_thread.RunEC(db, core, thread_c, 1, opss, utils.Core_opps())
		fmt.Printf("thread: %v\ttps: %v\t\n", thread_c , tps / 1000000)
	}

}