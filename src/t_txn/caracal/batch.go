package caracal

import (
	"container/list"
	"sync"
	"t_txn"
)

type Batch struct {
	opss *list.List 	//*Coroutine
	cur_ele *list.Element // not allocate
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


func NewBatch(opss *list.List, bohm *BOHM) *Batch {
	coros := list.New()
	txn_id := 0
	for ele := opss.Front(); ele != nil; ele = ele.Next() {
		txn_id ++ 
		acc := ele.Value.(t_txn.AccessPtr)
		coro := NewCoroutine(acc, txn_id, bohm, nil)
		coros.PushBack(coro)
	}
	return &Batch{coros, coros.Front(), &(sync.RWMutex{})}
}


func (batch *Batch) Reset() {
	batch.rwlock.Lock()
	defer batch.rwlock.Unlock()
	batch.cur_ele = batch.opss.Front()
}

