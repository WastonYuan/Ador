package t_coro

import (
	"t_txn"
	"t_coro/sgpm"
	// "time"
	"t_log"
	"sync"
	"t_coro/utils"
	// "fmt"
)

/*
get the opss and system parameter and let the thread run
return is tps reset_count(RR, ML) (conflict_count - reset_count)(AG + SF)
*/
func Run(db t_txn.DatabasePtr, core_cnt, thread_cnt, p_size int, opss [](*t_txn.OPS), core_opps float64) (float64, int, int) {
	
	t_count := len(opss)

	// thread := make(chan int, thread_c)
	thread := utils.NewSignal(thread_cnt)

	bm := sgpm.NewBatchManager()
	
	gq := bm.NewGlobalQueue(t_count)

	// new the coroutine and add to first batch
	for i := 0; i < t_count; i++ {
		ops := opss[i]
		txn := db.NewTXN(i, ops)
		coro := sgpm.NewCoroutine(i, txn, ops)
		if gq.Insert(coro) == false {
			// i--
		}
	}


	
	// start := time.Now()
	// t_log.Log(t_log.INFO, "start time: %v\n", start)
	
	
	max_opc_count := 0
	all_batch_reset_cnt := 0
	all_batch_conflict_cnt := 0
	utils.Core_opps()
	core_used := thread_cnt 
	if core_cnt < thread_cnt {
		core_used = core_cnt
	}
	batch_i := 0
	for true {
		batch_i ++
		db.Reset()
		// for each batch
		ngq := bm.Next()
		if ngq == nil {
			break
		}

		opcs := make(chan int, thread_cnt)
		reset_cnt := make(chan int, thread_cnt)
		conflict_cnt := make(chan int, thread_cnt)
		var wg sync.WaitGroup 

		var join_thread_start sync.WaitGroup 
		join_thread_start.Add(thread_cnt)

		for i := 0; i < thread_cnt; i ++ {
			wg.Add(1)
			go func(tid int) {
				join_thread_start.Done()
				join_thread_start.Wait()
				defer wg.Done()
				t := sgpm.NewThread(tid, ngq, p_size)
				thread.Add()
				// t_log.Log(t_log.DEBUG, "thread run: %v\n", tid)
				t.Run()
				thread.Release()
				opcs <- t.GetOpCnt()
				reset_cnt <- t.GetResetCnt()
				conflict_cnt <- t.GetConflictCnt()
			}(i)
		}
		wg.Wait()


		// elapsed := time.Since(start)
		close(opcs)
		max_opc := -1
		for opc := range opcs {
    	    // t_log.Log(t_log.INFO,"%v \n", opc)
			if opc > max_opc {
				max_opc = opc
			}
    	}

		close(reset_cnt)
		totall_reset_cnt := 0
		for cnt := range reset_cnt {
			totall_reset_cnt = totall_reset_cnt + cnt
		}
		all_batch_reset_cnt = all_batch_reset_cnt + totall_reset_cnt

		close(conflict_cnt)
		totall_conflict_cnt := 0
		for cnt := range conflict_cnt {
			totall_conflict_cnt = totall_conflict_cnt + cnt
		}
		all_batch_conflict_cnt = all_batch_conflict_cnt + totall_conflict_cnt

		max_opc_count = max_opc_count + max_opc
		t_log.Log(t_log.INFO, "batch %v ok\n", batch_i)
	}
	max_opc_count = max_opc_count + db.PreparationCost(thread_cnt, opss)
	return float64(t_count) / ( (float64(max_opc_count) /  (float64(core_opps) * float64(core_used) / float64(thread_cnt) ))), all_batch_reset_cnt, all_batch_conflict_cnt - all_batch_reset_cnt
}