package thread_model

import (
	"sync"
	"container/list"
	// "t_log"
	"t_txn"
	"fmt"
)



type ThreadManager struct {

	threads *([](*Thread))
	thread_cnt int
	batch *list.List
	rwlock *sync.RWMutex
	batch_lock *sync.RWMutex
	
}


func (tm *ThreadManager) PopCoroutineFromBatch() *Coroutine {
	tm.batch_lock.Lock()
	defer tm.batch_lock.Unlock()
	ele := tm.batch.Front()
	if ele != nil {
		coro := ele.Value.(*Coroutine)
		tm.batch.Remove(ele)
		return coro
	} else {
		return nil
	}
}

func (tm *ThreadManager) MakeCoroutine(c_id int, txn t_txn.TxnPtr, ops t_txn.AccessPtr) {
	coro := Coroutine{c_id, txn, ops, nil}
	tm.batch.PushBack(&coro)
}

func (tm *ThreadManager) BatchLen() int {
	return tm.batch.Len()
}

func NewThreadManager(thread_cnt int) (tm *ThreadManager) {
	// t_log.Log(t_log.DEBUG, "thread_cnt: %d\n", thread_cnt)
	vec_threads := make([](*Thread), thread_cnt)
	return &(ThreadManager{&vec_threads, thread_cnt, list.New(), &(sync.RWMutex{}), &(sync.RWMutex{})})

}


func (tm *ThreadManager) InitThreads() {
	tm.rwlock.Lock()
	defer tm.rwlock.Unlock()
	// t_log.Log(t_log.DEBUG, "len(*(tm.threads)) : %d， cap(*(tm.threads)) : %d\n", len(*(tm.threads)), cap(*(tm.threads)))
	for i := 0; i < len(*(tm.threads)); i++ {
		(*tm.threads)[i] = NewThread(i, tm)
	}
}

/*
must behind InitThreads
*/
func (tm *ThreadManager) GetThread(i int) (t *Thread) {

	tm.rwlock.RLock()
	defer tm.rwlock.RUnlock()
	return (*(tm.threads))[i]

}

/*
Get all coro from thread and distribute again
*/
func (tm *ThreadManager) Rebalance() {
	
	for i :=0 ; i < tm.thread_cnt; i++ {
		coros := tm.GetThread(i).coros
		for coro_ele := coros.Front(); coro_ele != nil; coro_ele = coro_ele.Next() {
			coro := coro_ele.Value.(*Coroutine)
			tm.batch.PushBack(coro)
		}
		coros.Init()
	}

	to_thread_id := 0
	for coro_ele := tm.batch.Front(); coro_ele != nil; coro_ele = coro_ele.Next() {
		coro := coro_ele.Value.(*Coroutine)
		thread := tm.GetThread(to_thread_id % tm.thread_cnt)
		thread.coros.PushBack(coro)
		coro.SetThread(thread)
		to_thread_id ++
	}
	tm.batch.Init()
}


// func (tm *ThreadManager) Resemble() {
// 	for i :=0 ; i < tm.thread_cnt; i++ {
// 		coros := tm.GetThread(i).coros
// 		batch := tm.batch
// 		for coro_ele := coros.Front(); coro_ele != nil; coro_ele = coro_ele.Next() {
// 			coro := coro_ele.Value.(*Coroutine)
// 			for b_ele := batch.Front(); b_ele != nil; b_ele = b_ele.Next() {
// 				b_coro := b_ele.Value.(*Coroutine)
// 				b_coro. 
// 			}
			
// 			tm.batch.PushBack(coro)
// 		}
// 		coros.Init()
// 	}

// }

func (tm *ThreadManager) StrThread2CoroCnt() string {
	str := ""
	for tid := 0; tid < tm.thread_cnt; tid ++ {
		thread := tm.GetThread(tid)
		str = str + fmt.Sprintf("[t:%d|c:%d] ", tid, thread.coros.Len())
	}
	return str
}