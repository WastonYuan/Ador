package sgpm

import (
	"t_txn"
	// "container/list"
	"t_log"
	// "time"
	"t_coro/sgpm/transfer"
)


/*
Context(coroutine) save entire ops and current operation index and Txn
ourside should init all txn to corotine before
*/
type Coroutine struct {
	coro_id int
	txn t_txn.TxnPtr
	ops *t_txn.OPS // ops has stats so do not need to save the current ptr
	Transfer int
	runningT *Thread
}

func (c *Coroutine) SetThread(t *Thread) {
	c.runningT = t
}

/*
used by thread
false means there are conflict
true means the transaction is finished
*/
func (coro *Coroutine) Run() int {
	coro.txn.Init()
	for true {
		op := coro.ops.Get()
		if op != nil {
			// t_log.Log(t_log.INFO, "do read write")
			var res int
			if op.Is_write == true {
				// value should in op!
				res = coro.txn.Write(op.Key)
			} else {
				res = coro.txn.Read(op.Key)
			}
			coro.runningT.read_write_cnt ++
			if res == t_txn.NEXT {
				// t_log.Log(t_log.DEBUG, "%v continue run", coro.coro_id)
				coro.ops.Next()
			} else {
				coro.runningT.conflict_cnt ++
				return res
			}
		} else { // the ops is being complete
			return coro.Commit()
		}
	}
	t_log.Log(t_log.PANIC, "ERROR position in coro run")
	return t_txn.NEXT
}

func (coro *Coroutine) Reset() int {
	coro.runningT.reset_cnt ++
	coro.ops.Reset()
	return coro.txn.Reset()
}

func (coro *Coroutine) SFReset() int {
	coro.runningT.reset_cnt ++
	coro.runningT.read_write_cnt = coro.runningT.read_write_cnt + coro.ops.GetCurrentIndex()
	return coro.txn.Reset()
}

func (coro *Coroutine) Commit() int {
	coro.runningT.commit_cnt ++
	c_res := coro.txn.Commit()
	if c_res != t_txn.NEXT {
		coro.runningT.conflict_cnt ++
	}
	// coro.ops.Commit() // for TPCC  
	return c_res
}


/*
txn is init in ourside, coroutine only focus on schedule
New Coroutine to a list then transfer to Thread
*/
func NewCoroutine(c_id int, txn t_txn.TxnPtr, ops *t_txn.OPS) *Coroutine {
	return &Coroutine{c_id, txn, ops, transfer.NONE, nil}
}


type Thread struct {
	tid int
	pq *ProcessQueue // current runing txn(Context)
	commit_cnt int
	read_write_cnt int
	reset_cnt int
	conflict_cnt int
}

func NewThread(tid int, gq *GlobalQueue, n int) *Thread {
	return &Thread{tid, NewProcessQueue(gq, n, tid), 0, 0, 0, 0}
}


func (t *Thread) GetOpCnt() int {
	return t.commit_cnt + t.read_write_cnt + t.reset_cnt
}

func (t *Thread) GetConflictCnt() int {
	return t.conflict_cnt
}

func (t *Thread) GetResetCnt() int {
	return t.reset_cnt
}

/*
use for oursider
*/
func (t *Thread) Run() {
	t_log.Log(t_log.DEBUG, "thread %v begin run\n", t.tid)
	for true {
		coro := t.pq.Schedule()
		if coro == nil {
			t_log.Log(t_log.INFO, "read_write_count: %v, commit_count: %v, reset_count: %v conflict_count: %v\n", t.read_write_cnt, t.commit_cnt, t.reset_cnt, t.conflict_cnt)
			return
		}
		coro.SetThread(t)
		// t_log.Log(t_log.DEBUG, "coro %v begin run\n", coro.coro_id)
		res := coro.Run()
		if res == t_txn.NEXT {
			coro.Transfer = transfer.DONE
			// t_log.Log(t_log.DEBUG, "time: %v, coro %v ok by thread %v\n", time.Now(), coro.coro_id ,t.tid)
		} else if res == t_txn.AGAIN { // agin then to schedule
			t_log.Log(t_log.DEBUG, "coro %v agin\n", coro.coro_id)
			continue
		} else if res == t_txn.RERUN {
			// t_log.Log(t_log.DEBUG, "coro %v rerun\n", coro.coro_id)
			coro.Reset()
		} else if res == t_txn.RE2FIRST {
			// calvin need to do nothing
			coro.SFReset()
			t_log.Log(t_log.DEBUG, "coro %v transfer first\n", coro.coro_id)
			coro.Transfer = transfer.SWAPFIRST
		} else if res == t_txn.NEXTBATCH {
			coro.Reset()
			coro.Transfer = transfer.NEXTBATCH
		} else if res == t_txn.RE2LAST {
			t_log.Log(t_log.INFO, "coro %v transfer last\n", coro.coro_id)
			coro.Reset()
			coro.Transfer = transfer.TOGLAST
		}
	}
	
}

