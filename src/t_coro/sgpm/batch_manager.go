package sgpm

import (
	"sync"
	"container/list"
)

/*
global queue list
one batch finished will remove from the list
batch is the list.Element of coroutine
*/

type BatchManager struct {
	bl *list.List // *GlobalQueue
	cur_batch *list.Element
	rwlock *sync.RWMutex
}

func (bm *BatchManager) NewGlobalQueue(size int) *GlobalQueue {
	ngq := GlobalQueue{list.New(), true, size, bm, &(sync.RWMutex{})}
	bm.bl.PushBack(&ngq)

	return &ngq
}


/*
first use return Front
last use return nil
last use again return front
if empty return nil
*/
func (bm *BatchManager) Next() *GlobalQueue {
	if bm.cur_batch == nil {
		bm.cur_batch = bm.bl.Front()
		if bm.cur_batch == nil {
			return nil
		} else {
			return bm.cur_batch.Value.(*GlobalQueue)
		}
	} else {
		bm.cur_batch = bm.cur_batch.Next()
		if bm.cur_batch == nil {
			return nil
		} else {
			return bm.cur_batch.Value.(*GlobalQueue)
		}

	}
}

func NewBatchManager() *BatchManager {
	return &BatchManager{list.New(), nil, &(sync.RWMutex{})}
}


func (bl *BatchManager) InsertNextBatch(coro *Coroutine) {
	bl.rwlock.Lock()
	defer bl.rwlock.Unlock()
	nb := bl.cur_batch.Next()
	var ngq *GlobalQueue
	cgq := bl.cur_batch.Value.(*GlobalQueue)
	size := cgq.GetSize()
	if nb == nil {
		ngq = bl.NewGlobalQueue(size)
	} else {
		ngq = nb.Value.(*GlobalQueue)
	}
	ngq.Insert(coro)
}

