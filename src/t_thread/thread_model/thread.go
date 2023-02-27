package thread_model

import (
	"container/list"
	"sync"
	"t_txn"
	"t_log"
)

type Thread struct {
	tid int
	thread_manager *ThreadManager
	coros *list.List // elemet is *coro
	cur_ele *list.Element

	read_write_cnt int
	commit_cnt int
	// for gria and aria
	raw_conflict_cnt int
	cascading_conflict_cnt int
	waw_conflict_cnt int

	// for other (wound-wait)
	block_cnt int

	reorder_cnt int

	// fallback strategy
	fb_read int
	fb_commit int
	fb_abort int
	fb_block int

	rwlock *(sync.Mutex)
	
}


func (t *Thread) GetFBBlockCnt() int {
	return t.fb_block
}


func (t *Thread) GetFBReadCnt() int {
	return t.fb_read
}

func (t *Thread) GetFBCommitCnt() int {
	return t.fb_commit
}

func (t *Thread) GetFBAbortCnt() int {
	return t.fb_abort
}


func (t *Thread) GetReorderCnt() int {
	return t.reorder_cnt
}


func (t *Thread) GetBlockCnt() int {
	return t.block_cnt
}

func (t *Thread) GetReadWriteCnt() int {
	return t.read_write_cnt
}

func (t *Thread) GetRAWConflictCnt() int {
	return t.raw_conflict_cnt
}

func (t *Thread) GetWAWConflictCnt() int {
	return t.waw_conflict_cnt
}

func (t *Thread) GetCascadingConflictCnt() int {
	return t.cascading_conflict_cnt
}

func (t *Thread) GetCommitCnt() int {
	return t.commit_cnt
}


func (t *Thread) StatsClear() {
	t.read_write_cnt = 0
	t.commit_cnt = 0
	t.raw_conflict_cnt = 0
	t.cascading_conflict_cnt = 0
	t.waw_conflict_cnt = 0

	t.block_cnt = 0

	t.reorder_cnt = 0

	t.fb_read = 0
	t.fb_commit = 0
	t.fb_abort = 0
	t.fb_block = 0
}



func NewThread(tid int, tm *ThreadManager) *Thread {
	return &Thread{tid, tm, list.New(), nil, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, &(sync.Mutex{})}
}

func (pq *Thread) Schedule() *Coroutine {
	pq.rwlock.Lock()
	defer pq.rwlock.Unlock() // not the must
	if pq.cur_ele == nil { // to the end and back to first
		pq.cur_ele = pq.coros.Front()
		
	} else { // not the end and to the next
		// check the cur is done if done then remove it
		pq.cur_ele = pq.cur_ele.Next()
	}
	if pq.cur_ele == nil {
		return nil
	}
	return pq.cur_ele.Value.(*Coroutine)
}


func (t *Thread) PushCoro(coro *Coroutine) {
	t.rwlock.Lock()
	defer t.rwlock.Unlock()
	t.coros.PushBack(coro)
}


// phase 1 have no aborted
// coro in a thread is run in sequence
func (t *Thread) Exec_Phase() {
	for ele := t.coros.Front(); ele != nil; ele = ele.Next() {

		coro := ele.Value.(*Coroutine)
		for true {
			res := coro.Exec()
			if res == t_txn.NEXT {
				break
			}
		}
	}
}



// phase 2 aboted the commit coro (del the coro from thread)
// coro in a thread is commit in commit_id sequence
func (t *Thread) Commit_Phase() {
	del_set := map[(*(list.Element))]bool{}
	for ele := t.coros.Front(); ele != nil; ele = ele.Next() {
		
		coro := ele.Value.(*Coroutine)
		res := coro.Commit()

		if res == t_txn.NEXT || res == t_txn.RO_NEXT {
			del_set[ele] = true
			if res == t_txn.RO_NEXT {
				t.reorder_cnt ++
			}
		}
		// } else {
		// 	coro.Reset()
		// }

	}

	for ele := range del_set {
		t.coros.Remove(ele)
	}

}

func (t *Thread) CorosReset() {
	for ele := t.coros.Front(); ele != nil; ele = ele.Next() {
		coro := ele.Value.(*Coroutine)
		coro.Reset()
	}
}



func (t *Thread) FallBack_Phase() {

	del_set := map[(*(list.Element))]bool{}
	for ele := t.coros.Front(); ele != nil; ele = ele.Next() {
		
		coro := ele.Value.(*Coroutine)
		res := coro.FallBack()
		// t_log.Log(t_log.INFO, "%v\n", res)
		if res == t_txn.FB_COMMIT {
			t.fb_commit ++
			del_set[ele] = true
		} else if res == t_txn.FB_ABORT {
			t.fb_abort ++
		} else {
			t_log.Log(t_log.INFO, "error type %v\n", res)
		}

	}

	for ele, _ := range del_set {
		t.coros.Remove(ele)
	}

}


func (t *Thread) Exec_Commit_Phase() {
	for true {
		if t.coros.Len() == 0 {
			n_coro := t.thread_manager.PopCoroutineFromBatch()
			if n_coro == nil {
				break
			} else {
				t.coros.PushBack(n_coro)
			}
		}
		ele := t.coros.Front()
		coro := ele.Value.(*Coroutine)
			
		coro.SetThread(t)
		for true {
			res := coro.Exec()
			if res == t_txn.NEXT {
				break
			} else if res == t_txn.AGAIN { // for calvin bohm and pwv
				t.block_cnt ++
				continue
			}
		}
		for true {
			res := coro.Commit()
			
			if res == t_txn.NEXT {
				// t_log.Log(t_log.INFO, "ok coro %v\n", coro.coro_id)
				t.coros.Remove(ele)
				break
			} else if res == t_txn.AGAIN { // for calvin bohm and pwv
				t.block_cnt ++
				continue
			}
		}
	}
	
}

func (t *Thread) CoroLeft() int {
	return t.coros.Len()
}

