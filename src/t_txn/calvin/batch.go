package calvin

import (
	"container/list"
	"sync"
	"t_txn"
)

type Batch struct {
	opss *list.List 	//*Coroutine
	cur_ele *list.Element // not allocate
	w_sum int
	rwlock *sync.RWMutex

}

func (batch *Batch) Pop() *Coroutine {
	batch.rwlock.Lock()
	defer batch.rwlock.Unlock()
	if batch.cur_ele != nil {
		coro := batch.cur_ele.Value.(*Coroutine)
		// batch.w_sum =  batch.w_sum + len(*(coro.txn.write_set))
		batch.cur_ele = batch.cur_ele.Next()
		return coro
	}
	return nil
}


func NewBatch(opss *list.List, calvin *Calvin) *Batch {
	coros := list.New()
	txn_id := 0
	w_sum := 0
	for ele := opss.Front(); ele != nil; ele = ele.Next() {
		txn_id ++ 
		acc := ele.Value.(t_txn.AccessPtr)
		coro := NewCoroutine(acc, txn_id, calvin, nil)
		w_sum =  w_sum + len(*(coro.txn.write_set))
		coros.PushBack(coro)
	}
	return &Batch{coros, coros.Front(), w_sum, &(sync.RWMutex{})}
}


func (batch *Batch) Reset() {
	batch.rwlock.Lock()
	defer batch.rwlock.Unlock()
	batch.cur_ele = batch.opss.Front()
}


func (batch *Batch) GetFStep() int {
	batch.rwlock.RLock()
	defer batch.rwlock.RUnlock()
	return batch.w_sum
}