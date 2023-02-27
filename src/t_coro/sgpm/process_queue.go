package sgpm

import (
	"container/list"
	"t_log"
	"t_coro/sgpm/transfer"
)

type ProcessQueue struct {
	coros *list.List // elemet is *coro
	gq *GlobalQueue
	size int
	cur_ele *list.Element
	tid int
}


func NewProcessQueue(gq *GlobalQueue, size int, tid int) *ProcessQueue {
	return &ProcessQueue{list.New(), gq, size, nil, tid}
}


func (pq *ProcessQueue) IsFull() bool {
	return pq.coros.Len() >= pq.size 
}

/*
global empty or prcess full all will return false
This method should be used sparingly because it lock the global queue
*/
func (pq *ProcessQueue) pushFromGlobal() bool {
	gq := pq.gq
	gq.rwlock.Lock()
	defer gq.rwlock.Unlock()
	front := gq.coros.Front()
	if front == nil { // gq is empty
		gq.IsEmpty = true
		return false
	}
	coro := front.Value.(*Coroutine)
	 
	if pq.Insert(coro) {
		gq.coros.Remove(front)
		return true
	} else {
		return false
	}
}


func (pq *ProcessQueue) Insert(coro *Coroutine) bool {
	if pq.coros.Len() >= pq.size {
		return false
	} else {
		pq.coros.PushBack(coro)
		return true
	}
}


/*
return the next running coro
nil means there is no coro to run, the thread can be exist
*/
func (pq *ProcessQueue) Schedule() *Coroutine {
	for true {
		if !pq.IsFull() && !pq.gq.IsEmpty {
			// t_log.Log(t_log.INFO, "tid %v get global\n", pq.tid)
			pq.pushFromGlobal()
		}
		if pq.cur_ele == nil { // to the end and back to first
			pq.cur_ele = pq.coros.Front()
			if pq.cur_ele == nil { // no coro but push fail(global is empty) so the process is nothing can do
				return nil
			}
		} else { // not the end and to the next
			// check the cur is done if done then remove it
			pre_ele := pq.cur_ele
			pre_coro := pre_ele.Value.(*Coroutine)
			pq.cur_ele = pq.cur_ele.Next()
			if pre_coro.Transfer == transfer.DONE {
				pre_coro.Transfer = transfer.NONE
				pq.coros.Remove(pre_ele) // the coro will remove forever so transfer is no need to care
			} else if pre_coro.Transfer == transfer.SWAPFIRST {
				pre_coro.Transfer = transfer.NONE
				f_coro := pq.gq.SwapFront(pre_coro)
				pq.coros.InsertAfter(f_coro, pre_ele) 

				pq.coros.Remove(pre_ele)
			} else if pre_coro.Transfer == transfer.NEXTBATCH {
				pre_coro.Transfer = transfer.NONE
				pq.gq.bm.InsertNextBatch(pre_coro)
				pq.coros.Remove(pre_ele)
			} else if pre_coro.Transfer == transfer.TOGLAST {
				pre_coro.Transfer = transfer.NONE
				pq.coros.Remove(pre_ele)
				pq.gq.InsertLast(pre_coro)
				t_log.Log(t_log.INFO, "gq_len: %v\n", pq.gq.Len())
			}
			if pq.cur_ele == nil { // next is the end
				continue
			}
		}
		return pq.cur_ele.Value.(*Coroutine)
	}
	t_log.Log(t_log.PANIC, "ERROR position in schedule\n")
	return nil
}
