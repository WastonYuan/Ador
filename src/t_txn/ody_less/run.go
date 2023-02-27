package ody_less


import (
	"container/list"
	"sync"
	"t_log"
	"t_txn"
)

var cc *CC = nil
var core_cnt int = 16
var global_thread_cnt int = 1 // read only variable
var global_batch_cnt int = 0
var QueueSize int = 1
var DelayExecCnt int = 0 // set to 0 then there is only commit delay
var DelayCommitCnt int = 8
// list with t_txn.AccessPtr
func Run(opss *list.List, thread_cnt int) {

	cc = NewCC()
	MaxCommitID = -1
	MaxDoneExecID = -1
	MaxCommitIDLock = sync.RWMutex{}

	DelayCommitCnt = thread_cnt // there is no delay exec

	global_thread_cnt = thread_cnt
	global_batch_cnt = (opss.Len() + thread_cnt - 1) / thread_cnt
	// 1. init thread
	threads := make([](*Thread), thread_cnt)
	for i := 0; i < thread_cnt; i ++ {
		threads[i] = NewThread(i)
	}
	// 2. New a database
	ody := NewOdy()
	
	// 3. round robin fashion assign coro
	max_txn_id := 0 // the txn_id is also begin from 0
	thd_i := 0
	for ele := opss.Front(); ele != nil; ele = ele.Next() { 
		// 3.1 create coro (transaction)
		ops := ele.Value.(t_txn.AccessPtr) 
		coro := NewCoroutine(ops, max_txn_id, ody)
		max_txn_id ++
		// 3.2 coro assign to thread in round robin fashion
		cur_thd := thd_i % thread_cnt
		threads[cur_thd].PushCoro(coro)
		thd_i ++
	}
	t_log.Log(t_log.DEBUG, "max txn_id: %v, global_batch_cnt :%v\n", max_txn_id, global_batch_cnt)
	// 4. run each thread with one phase 
	var wg sync.WaitGroup
	wg.Add(thread_cnt)
	cc.Start(thread_cnt)
	for i := 0; i < thread_cnt; i ++ {
		go func(t *Thread) {
			defer wg.Done()
			t_log.Log(t_log.DEBUG, "thread_%v start\n", t.ThreadID)
			t.Run()
			t_log.Log(t_log.DEBUG, "thread_%v completed\n", t.ThreadID)
		}(threads[i])
	}
	wg.Wait()
	t_log.Log(t_log.INFO, "%v\n", StatisticsTitle())
	rw_sum := 0
	for i := 0; i < thread_cnt; i++ {
		rw_sum += (threads[i].statics.major_read_cnt + threads[i].statics.major_write_cnt)
		t_log.Log(t_log.INFO, "(%v)\t%v\n", i, threads[i].StatisticsResult())
	}
	t_log.Log(t_log.INFO, "%v\n", rw_sum)
}