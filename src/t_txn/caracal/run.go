package caracal


import (
	"container/list"
	"sync"
	"t_log"
)

var cc *CC = nil
const core_cnt = 16
// list with t_txn.AccessPtr
func Run(opss *list.List, thread_cnt int) {

	cc = NewCC()
	// make coroutine
	bohm := NewBOHM()
	

	batch := NewBatch(opss, bohm)

	// init thread
	threads := make([](*Thread), thread_cnt)
	for i := 0; i < thread_cnt; i ++ {
		threads[i] = NewThread(batch)
	}

	// run thread

	// first phase
	var wg sync.WaitGroup
	wg.Add(thread_cnt)
	for i := 0; i < thread_cnt; i ++ {
		go func(t *Thread) {
			defer wg.Done()
			t.RunFirstPhase()
		}(threads[i])
	}
	wg.Wait()


	batch.Reset()

	cc.Start(thread_cnt)
	// second phase
	wg.Add(thread_cnt)
	for i := 0; i < thread_cnt; i ++ {
		go func(t *Thread) {
			defer wg.Done()
			t.RunSecondPhase()
		}(threads[i])
	}
	wg.Wait()
	

	t_log.Log(t_log.INFO, "%v\n", StatisticsTitle())
	for i := 0; i < thread_cnt; i++ {
		t_log.Log(t_log.INFO, "%v\n", threads[i].StatisticsResult())
	}
}