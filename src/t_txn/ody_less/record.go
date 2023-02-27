package ody_less

import (
	"sync"
	// "container/list"
	// // "t_txn"
	// "t_log"
	// "fmt"
)


type Record struct {
	wts int // commit tid
	batches_wts  *([](*int))
	batches_lock *([](*sync.RWMutex))
	wts_lock *(sync.RWMutex)
}


func (r *Record) GetWts() int {
	r.wts_lock.RLock()
	defer r.wts_lock.RUnlock()
	return r.wts
}

func (r *Record) SetWts(wts int) {
	r.wts_lock.Lock()
	defer r.wts_lock.Unlock()
	r.wts = wts
}

func (r *Record) GetBatchWts(batch_i int) int {
	(*r.batches_lock)[batch_i].RLock()
	defer (*r.batches_lock)[batch_i].RUnlock()
	return *(*r.batches_wts)[batch_i]
}

func (r *Record) SetBatchWts(batch_i int, batch_wts int) {
	(*r.batches_lock)[batch_i].Lock()
	defer (*r.batches_lock)[batch_i].Unlock()
	batch_wts_p := (*r.batches_wts)[batch_i]
	*batch_wts_p = batch_wts
}



func batch_index (txn_id int) int {
	// if txn_id / global_thread_cnt >= 125 {
	// 	t_log.Log(t_log.DEBUG, "========== txn_id: %v, thread_cnt: %v =========", txn_id, global_thread_cnt)
	// }
	return txn_id / global_thread_cnt
}

func NewRecord() *Record {
	batch_wts := make([](*int), global_batch_cnt)
	for i := 0; i < global_batch_cnt; i++ { // init the thread_wts
		var t int = -1
		batch_wts[i] = &t
	}

	batch_lock := make([](*sync.RWMutex), global_batch_cnt)
	for i := 0; i < global_batch_cnt; i++ { // init the thread_wts
		var t sync.RWMutex
		batch_lock[i] = &t
	}

	return &(Record{-1, &batch_wts, &batch_lock, &sync.RWMutex{}})
}


/*
write must ok
*/
func (r *Record) Write(txn *TXN) {
	batch_i := batch_index(txn.txn_id)
	batch_wts := r.GetBatchWts(batch_i)
	if batch_wts < txn.txn_id {
		r.SetBatchWts(batch_i, txn.txn_id)
	}
}


/*
read must ok
*/
func (r *Record) Read(txn *TXN) int {
	wts := r.GetWts()
	return wts
}


func (r *Record) Commit(txn *TXN) {
	r.SetWts(txn.txn_id)
}


func (r *Record) IsReadable(txn *TXN) bool {
	return r.GetWts() < txn.txn_id
}


func (r *Record) IsEmpty() bool {
	return r.GetWts() == -1
}


func (r *Record) Validate(txn *TXN, rts int) bool {
	wts := r.GetWts()
	batch_i := batch_index(txn.txn_id)
	batch_wts := r.GetBatchWts(batch_i)
	if wts == rts {
		if batch_wts < wts { // batch_wts must = -1, since if write in the batch, the batch_wts must >= wts.
			r.SetBatchWts(batch_i, wts)
		}
		return true
	} else {	// reset the batch_wts
		// wts (committed) < rts <= batch_wts < txn_id. 
		// When conflict, if batch_wts == rts, the batch_wts is invalid (not be committed futher since this txn has larger txn_id is committed), 
		//		and the txn behind should not read this batch_wts.
		// When conflict, if batch_wts < wts, this batch has not been write yet (batch_wts must = -1), and then replace the batch_wts with wts. 
		if batch_wts == rts || batch_wts < wts {
			r.SetBatchWts(batch_i, wts)
		}
		return false
	}
}


