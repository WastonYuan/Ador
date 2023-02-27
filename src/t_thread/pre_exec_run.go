package t_thread

import (
	"t_txn"
	"t_thread/utils"
	"t_thread/thread_model"
	"sync"
	"t_log"
	
)
/* 
PE has only one batch
*/
func RunPE(db t_txn.DatabasePtr, core_cnt, thread_cnt, p_size int, opss [](t_txn.AccessPtr), core_opps float64) float64 {
	txn_count := len(opss)

	thread_sig := make(chan int, thread_cnt)

	thread_manager := thread_model.NewThreadManager(thread_cnt)

	thread_manager.InitThreads()

	// Phase 1
	// t_log.Log(t_log.INFO, "txn_len:%v\n", txn_count)
	for i := 0; i < txn_count; i++ {
		ops := opss[i]
		txn := db.NewTXN(i, ops)
		// store the txns by batch
		thread_manager.MakeCoroutine(i, txn, ops)
	}
	

	// t_log.Log(t_log.INFO, "txn_len:%v\n", thread_manager.BatchLen())
	// rebalance is also used for calvin bohm and pwv since the coro is assign by order to all thread
	max_opc_count := 0 // for sum the totall opc
	utils.Core_opps()
	core_used := thread_cnt 
	if core_cnt < thread_cnt {
		core_used = core_cnt
	}
	// range batch


	totall_rw_cnt := 0
	totall_block_cnt := 0
	totall_commit_cnt := 0


	db.Reset()

	var join_thread_start sync.WaitGroup 
	//  phase 2
	var wg sync.WaitGroup 
	
	join_thread_start.Add(thread_cnt)
	

	for i := 0; i < thread_cnt; i ++ {
		wg.Add(1)
		go func(tid int) {
			join_thread_start.Done()
			join_thread_start.Wait()
			defer wg.Done()

			t := thread_manager.GetThread(tid)
			thread_sig <- 0
			t.Exec_Commit_Phase() 
			<- thread_sig
		}(i)
	}
	wg.Wait()
 
	// stats
	max_batch_opc := -1
	
	// just for stats
	for i := 0; i < thread_cnt; i ++ {

		t := thread_manager.GetThread(i)

		if t.CoroLeft() != 0 {
			t_log.Log(t_log.DEBUG, "thread :%d, left %d\n", i, t.CoroLeft())
		} else {
			t_log.Log(t_log.DEBUG, "thread :%d, commit done\n", i)
		}

		thread_opc := t.GetReadWriteCnt() - t.GetBlockCnt() + t.GetCommitCnt() + t.GetBlockCnt() / 100

		if thread_opc > max_batch_opc {
			max_batch_opc = thread_opc
		}
		// stats
		totall_rw_cnt = totall_rw_cnt + t.GetReadWriteCnt()
		totall_block_cnt = totall_block_cnt + t.GetBlockCnt()
		totall_commit_cnt = totall_commit_cnt + t.GetCommitCnt()
		t.StatsClear()

	}


	max_opc_count = max_opc_count + max_batch_opc


	// max_opc_count = max_opc_count + db.PreparationCost(thread_cnt, opss)
	t_log.Log(t_log.INFO, "rw_cnt:%d\tblock_cnt:%d\t", totall_rw_cnt + totall_commit_cnt - totall_block_cnt, totall_block_cnt / 3)
	return float64(txn_count) / ( (float64(max_opc_count) /  (float64(core_opps) * float64(core_used) / float64(thread_cnt) )))
}
 