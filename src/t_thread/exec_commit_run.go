package t_thread

import (
	"t_txn"
	"t_thread/utils"
	"t_thread/thread_model"
	"sync"
	"t_log"
	"t_stats"
	"time"
	"t_util"
	
)
/*
get the opss and system parameter and let the thread run
return is tps reset_count(RR, ML) (conflict_count - reset_count)(AG + SF)
*/
func RunEC(db t_txn.DatabasePtr, core_cnt, thread_cnt, rw_cost int, opss [](t_txn.AccessPtr), core_opps float64) float64 {
	txn_count := len(opss)

	thread_sig := make(chan int, thread_cnt)

	thread_manager := thread_model.NewThreadManager(thread_cnt)

	thread_manager.InitThreads()

	// new the coroutine and add to first batch
	for i := 0; i < txn_count; i++ {
		ops := opss[i]
		txn := db.NewTXN(i, ops)
		// store the txns by batch
		thread_manager.MakeCoroutine(i, txn, ops)
	}
	s := time.Now()
	thread_manager.Rebalance()
	t_stats.Rebalance = t_stats.Rebalance  + time.Since(s)


	max_opc_count := 0 // for sum the totall opc
	utils.Core_opps()
	core_used := thread_cnt 
	if core_cnt < thread_cnt {
		core_used = core_cnt
	}
	// range batch


	totall_raw_cnt := 0
	totall_waw_cnt :=0
	totall_cas_cnt := 0
	totall_batch_cnt := 0
	totall_commit_cnt := 0
	totall_reoder_cnt := 0

	totall_fb_read_cnt := 0
	totall_fb_commit_cnt := 0
	totall_fb_abort_cnt := 0
	totall_fb_block_cnt := 0

	for true {
		s := time.Now()
		totall_batch_cnt ++
		db.Reset()
		// for each batch

		t_log.Log(t_log.DEBUG, "%v\n", thread_manager.StrThread2CoroCnt())
		var join_thread_start sync.WaitGroup 
		// phase 1
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
				t.Exec_Phase() 
				<- thread_sig
			}(i)
		}
		wg.Wait()
		t_stats.Execution_phase = t_stats.Execution_phase  + time.Since(s)

		// phase 2
		s = time.Now()
		join_thread_start.Add(thread_cnt)

		for i := 0; i < thread_cnt; i ++ {
			wg.Add(1)
			go func(tid int) {
				join_thread_start.Done()
				join_thread_start.Wait()
				defer wg.Done()

				t := thread_manager.GetThread(tid)
				thread_sig <- 0
				t.Commit_Phase()
				<- thread_sig
			}(i)
		}
		wg.Wait()

		// fallback phase

		// thread_manager.Resemble()
		if t_util.Pconf.Fallback && t_util.Pconf.Server == "Gria" {
			join_thread_start.Add(thread_cnt)
			for i := 0; i < thread_cnt; i ++ {
				wg.Add(1)
				go func(tid int) {
					join_thread_start.Done()
					join_thread_start.Wait()
					defer wg.Done()

					t := thread_manager.GetThread(tid)
					thread_sig <- 0
					t.FallBack_Phase()
					<- thread_sig
				}(i)
			}
			wg.Wait()
		}
		
		// stats
		done := true
		max_batch_opc := -1

		batch_commitc := 0
		batch_raw_conflict := 0
		batch_waw_conflict := 0
		batch_cascading_conflict := 0
		batch_reoder_cnt := 0

		batch_fb_read_cnt := 0
		batch_fb_commit_cnt := 0
		batch_fb_abort_cnt := 0
		batch_fb_block_cnt := 0

		

		for i := 0; i < thread_cnt; i ++ {

			t := thread_manager.GetThread(i)
			//Reset
		
			t.CorosReset()

			if t.CoroLeft() != 0 {
				done = false
				t_log.Log(t_log.DEBUG, "thread :%d, left %d\n", i, t.CoroLeft())
			} else {
				t_log.Log(t_log.DEBUG, "thread :%d, commit done\n", i)
			}

			

			if t.GetReadWriteCnt() + t.GetFBReadCnt() + t.GetFBBlockCnt() / 100 > max_batch_opc {
				max_batch_opc = t.GetReadWriteCnt() + t.GetFBReadCnt() + t.GetFBBlockCnt() / 100
			}
			// stats
			batch_commitc = t.GetCommitCnt() + batch_commitc
			batch_raw_conflict = t.GetRAWConflictCnt() + batch_raw_conflict
			batch_waw_conflict = t.GetWAWConflictCnt() + batch_waw_conflict
			batch_cascading_conflict = t.GetCascadingConflictCnt() + batch_cascading_conflict
			batch_reoder_cnt = t.GetReorderCnt() + batch_reoder_cnt
			
			
			batch_fb_commit_cnt = t.GetFBCommitCnt() + batch_fb_commit_cnt
			batch_fb_abort_cnt = t.GetFBAbortCnt() + batch_fb_abort_cnt
			batch_fb_block_cnt = t.GetFBBlockCnt() + batch_fb_block_cnt


			t.StatsClear()

		}

		totall_raw_cnt = totall_raw_cnt + batch_raw_conflict
		totall_waw_cnt = totall_waw_cnt + batch_waw_conflict
		totall_commit_cnt = totall_commit_cnt + batch_commitc
		totall_cas_cnt = totall_cas_cnt + batch_cascading_conflict
		totall_reoder_cnt = totall_reoder_cnt + batch_reoder_cnt

		totall_fb_read_cnt = totall_fb_read_cnt + batch_fb_read_cnt
		totall_fb_abort_cnt = totall_fb_abort_cnt + batch_fb_abort_cnt
		totall_fb_commit_cnt = totall_fb_commit_cnt + batch_fb_commit_cnt
		totall_fb_block_cnt = totall_fb_block_cnt + batch_fb_block_cnt


		max_opc_count = max_opc_count + max_batch_opc
		// t_log.Log(t_log.INFO, "batch %v ok, txn_cnt: %d\n", totall_batch_cnt, batch_commitc)


		if done {
			break
		}
		t_stats.Commit_phase = t_stats.Commit_phase  + time.Since(s)
		/* rebalance start */
		s = time.Now()
		thread_manager.Rebalance()
		t_stats.Rebalance = t_stats.Rebalance  + time.Since(s)
		/* rebalance end */
		

	}
	max_opc_count = max_opc_count + db.PreparationCost(thread_cnt, opss)
	// t_log.Log(t_log.INFO, "%v %v\n", txn_count, max_opc_count, core_opps)
	t_log.Log(t_log.INFO, "	rw_cnt:%d\traw_cnt:%d\twaw_cnt:%d\tcas_cnt:%d\tbatch_cnt:%d\tcommit_cnt:%d\treorder_cnt:%d\tfb_read_cnt:%d\tfb_commit_cnt:%d\tfb_abort_cnt:%d\tfb_block_cnt:%v", max_opc_count, totall_raw_cnt, totall_waw_cnt, totall_cas_cnt, totall_batch_cnt, totall_commit_cnt, totall_reoder_cnt, totall_fb_read_cnt, totall_fb_commit_cnt, totall_fb_abort_cnt, totall_fb_block_cnt)
	return float64(txn_count) / ( (float64(max_opc_count) /  (float64(core_opps) / float64(rw_cost) * float64(core_used) / float64(thread_cnt) )))
}
