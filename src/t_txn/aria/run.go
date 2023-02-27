

package aria


import (
	"container/list"
	"sync"
	"t_log"
)


// list with t_txn.AccessPtr
func Run(opss *list.List, thread_cnt int) {

	batch := NewBatch(opss)

	aria := NewAria()

	const subBatch_size = 16
	subBatch := batch.NewSubBatch(aria, subBatch_size)

	threads := make([](*Thread), thread_cnt)

	for i := 0; i < thread_cnt; i ++ {
		threads[i] = NewThread(subBatch)
	}
	t_log.Log(t_log.INFO, "%v\n", StatisticsTitle())
	round := 0
	for !batch.IsEmpty() || !subBatch.IsEmpty() {

		round = round + 1
		subBatch.FetchFullFromBatch()

		// First phase
		var wg sync.WaitGroup
		wg.Add(thread_cnt)
		for i := 0; i < thread_cnt; i++ {
			go func(t *Thread) {
				defer wg.Done()
				t.RunFirstPhase()
			}(threads[i])
		}
		wg.Wait()


		// Second phase
		subBatch.Reset()
		wg.Add(thread_cnt)
		for i := 0; i < thread_cnt; i++ {
			go func(t *Thread) {
				defer wg.Done()
				t.RunSecondPhase()
			}(threads[i])
		}
		wg.Wait()

		t_log.Log(t_log.INFO, "batch %v:\n", round)
		for i := 0; i < thread_cnt; i ++ {
			t_log.Log(t_log.INFO, "%v\n", threads[i].StatisticsResult())
			threads[i].Reset()
		}
		
		subBatch.Reset()
		aria.Reset()
	}

}