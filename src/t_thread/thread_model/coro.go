package thread_model

import (
	"t_txn"
	// "container/list"
	"t_log"
)



type Coroutine struct {
	coro_id int
	txn t_txn.TxnPtr
	ops t_txn.AccessPtr // ops has stats so do not need to save the current ptr
	runningT *Thread
}

func (coro *Coroutine) SetThread(t *Thread) {
	coro.runningT = t
}


// for gria this means the transaction needs run in next batch
func (coro *Coroutine) Reset() {
	coro.txn.Reset()
	coro.ops.Reset()
}


func (coro *Coroutine) Exec() int {
	group_id := coro.runningT.tid
	coro.txn.Init(group_id)
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
				coro.ops.Next() 
			} else {
				// for Goria and Aria just has Next
				return res // record the conflict count
			}
		} else { // finished
			return t_txn.NEXT
		}
	}
	t_log.Log(t_log.PANIC, "ERROR position in coro run")
	return t_txn.NEXT
}


func (coro *Coroutine) Commit() int {
	c_res := coro.txn.Commit()
	if c_res == t_txn.NEXTBATCH_WAW || c_res == t_txn.NEXTBATCH_RAW || c_res == t_txn.NEXTBATCH_CA {
		// coro.Reset()
		// stats
		if c_res == t_txn.NEXTBATCH_RAW {
			coro.runningT.raw_conflict_cnt ++
		} else if c_res == t_txn.NEXTBATCH_CA {
			coro.runningT.cascading_conflict_cnt ++
		} else if c_res == t_txn.NEXTBATCH_WAW {
			coro.runningT.waw_conflict_cnt ++
		}
	}
	coro.runningT.commit_cnt ++
	return c_res
}


func (coro *Coroutine) FallBack() int {
	coro.runningT.fb_read ++
	c_res := coro.txn.FallBack()
	for true {
		if c_res == t_txn.FB_AGAIN_BLOCK || c_res == t_txn.FB_AGAIN {
			if c_res == t_txn.FB_AGAIN_BLOCK {
				coro.runningT.fb_block ++
			} else {
				coro.runningT.fb_read ++
			}
			c_res = coro.txn.FallBack()
		} else {
			break
		}
	}
	return c_res
}