package aria

import (
	"container/list"
	"t_txn"
	"sync"
)

type Batch struct {
	opss *list.List 	//t_txn.AccessPtr
	cur_ele *list.Element // not allocate
	txn_id int
	rwlock *sync.RWMutex

}

func (batch *Batch) Pop() (t_txn.AccessPtr, int) {
	batch.rwlock.Lock()
	defer batch.rwlock.Unlock()
	if batch.cur_ele != nil {
		batch.txn_id = batch.txn_id + 1
		acc := batch.cur_ele.Value.(t_txn.AccessPtr)
		batch.cur_ele = batch.cur_ele.Next()
		return acc, batch.txn_id
	}
	return nil, -1
}

func NewBatch(opss *list.List) *Batch {
	return &(Batch{opss, opss.Front(), 0, &(sync.RWMutex{})})
}

func (batch *Batch) IsEmpty() bool {
	batch.rwlock.RLock()
	defer batch.rwlock.RUnlock()
	return batch.cur_ele == nil
}

/*
Thread get coroutine from SubBatch
*/
type SubBatch struct {
	coros *list.List // *Coroutine
	cur_ele *list.Element // not allocate
	size int
	batch *Batch
	db *Aria
	rwlock *sync.RWMutex
}

func (batch *Batch) NewSubBatch(db *Aria, size int) *SubBatch {
	l := list.New()
	return &SubBatch{l, nil, size, batch, db, &sync.RWMutex{}}
}


func (sb *SubBatch) IsEmpty() bool {
	return sb.cur_ele == nil
}


func (sb *SubBatch) FetchFullFromBatch() {
	sb.rwlock.Lock()
	defer sb.rwlock.Unlock() 
	for sb.coros.Len() < sb.size {
		acc, txn_id := sb.batch.Pop()
		if acc == nil {
			break
		}
		coro := NewCoroutine(acc, txn_id, sb.db, nil)
		sb.coros.PushBack(coro)
	}
	sb.cur_ele = sb.coros.Front()
}


func (sb *SubBatch) Assign() *list.Element {
	sb.rwlock.Lock()
	defer sb.rwlock.Unlock() 
	res := sb.cur_ele
	if res != nil {
		sb.cur_ele = sb.cur_ele.Next()
	}
	return res
}

func (sb *SubBatch) Remove(ele *list.Element) {
	sb.rwlock.Lock()
	defer sb.rwlock.Unlock() 
	sb.coros.Remove(ele)
}

func (sb *SubBatch) Reset() {
	sb.rwlock.Lock()
	defer sb.rwlock.Unlock() 
	sb.cur_ele = sb.coros.Front()
}



