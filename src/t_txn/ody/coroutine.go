package ody

import (
	"t_txn"
	"sync"
	// "t_log"
)

/*
interface for thread:
Exec, Commit(thread judge), Abort.
The jobs of coroutine
1. statistics
2. save the context 
3. if commit fail then reset context
*/

var MaxCommitID int
var MaxCommitIDLock sync.RWMutex
var MaxDoneExecID int
var MaxDoneExecIDLock sync.RWMutex 

/*
the coroutine only access by one thread so do not need lock
*/
type Coroutine struct {
	context t_txn.AccessPtr
	txn *TXN
	thread *Thread
	is_running bool
	fail_point *t_txn.OP
}

// since txn and coroutine pointed to each other.
func (coro *Coroutine) SetTXN(txn *TXN) {
	coro.txn = txn
}

func NewCoroutine(ops t_txn.AccessPtr, txn_id int, db *Ody) *Coroutine {
	n_coro := Coroutine{ops, nil, nil, false, nil}
	txn := db.NewTXN(txn_id, &n_coro)
	n_coro.SetTXN(txn)
	return &n_coro
}

func (coro *Coroutine) IsRunning() bool {
	return coro.is_running
}

func (coro *Coroutine) IsCommitable() bool {
	MaxCommitIDLock.RLock()
	defer MaxCommitIDLock.RUnlock()
	txn_id := coro.GetTxnId()
	if txn_id == MaxCommitID + 1 {
		return true
	} else {
		return false
	}
}

func (coro *Coroutine) IsRunable() bool {
	MaxCommitIDLock.RLock()
	defer MaxCommitIDLock.RUnlock()
	// return coro.GetTxnId() <= MaxCommitID + DelayCnt 
	return coro.GetTxnId() <= MaxCommitID + DelayCommitCnt || coro.GetTxnId() <= MaxDoneExecID + DelayExecCnt
}


func (coro *Coroutine) DoneExec() {
	MaxDoneExecIDLock.Lock()
	defer MaxDoneExecIDLock.Unlock()
	txn_id := coro.GetTxnId()
	if MaxDoneExecID < txn_id {
		MaxDoneExecID = txn_id
	}
}

func (coro *Coroutine) SetThread(thread *Thread) {
	coro.thread = thread
}

func (coro *Coroutine) GetTxnId() int {
	return coro.txn.txn_id
}

// since only the former completed then the next coro start, the former of exec coro must be commitable.
// return false means complete exec
func (coro *Coroutine) Exec(is_major bool) (int, bool) {
	op := coro.context.Get()
	if op == nil { 
		return 0, false
	}
	coro.is_running = true
	var step int = 1
	if op.Type == t_txn.OP_READ {
		coro.txn.SingleRead(op.Key)
		coro.thread.statics.read_cnt = coro.thread.statics.read_cnt + 1
		if is_major {
			coro.thread.statics.major_read_cnt = coro.thread.statics.major_read_cnt + 1
		}
	} else if op.Type == t_txn.OP_WRITE {
		coro.txn.Write(op.Key)
		coro.thread.statics.write_cnt = coro.thread.statics.write_cnt + 1
		if is_major {
			coro.thread.statics.major_write_cnt = coro.thread.statics.major_write_cnt + 1
		}
	} else if op.Type == t_txn.OP_SCAN {
		// each scan trigger is one checkpoint no matter the length
		read_cnt := coro.txn.MultiRead(op.Key, op.Len)
		coro.thread.statics.read_cnt = coro.thread.statics.read_cnt + read_cnt
		if is_major {
			// t_log.Log(t_log.DEBUG, "run scan in major %v, read_cnt:%v\n", op, read_cnt)
			coro.thread.statics.major_read_cnt = coro.thread.statics.major_read_cnt + read_cnt
			step = read_cnt
		}
	}
	coro.context.Next()
	return step, true
}

/*
current commit txn fail need also commit
*/
func (coro *Coroutine) Abort() {
	t := coro.txn
	t.Abort()
	coro.context.Reset()
	coro.is_running = false
	coro.thread.statics.abort_cnt = coro.thread.statics.abort_cnt + 1
}


func (coro *Coroutine) Validate() bool {
	t := coro.txn
	key, s_len, is_p, ok := t.Validate()
	if ok == false {
		// save the fail point for rebase
		var op t_txn.OP
		if s_len == 0 {
			op = t_txn.OP{key, s_len, t_txn.OP_READ}
		} else {
			op = t_txn.OP{key, s_len, t_txn.OP_SCAN}
		}
		coro.fail_point = &op
		if is_p {
			coro.thread.statics.phantom_cnt = coro.thread.statics.phantom_cnt + 1
		}
		coro.thread.statics.conflict_cnt = coro.thread.statics.conflict_cnt + 1
	}
	return ok
}


func (coro *Coroutine) Rebase() {
	if coro.fail_point != nil {
		op := coro.fail_point
		coro.context.Revert(op)
		write_cnt := coro.txn.Rebase(op.Key, op.Len)
		coro.thread.statics.rebase_cnt = coro.thread.statics.rebase_cnt + 1
		coro.thread.statics.write_cnt = coro.thread.statics.write_cnt + write_cnt
		coro.thread.statics.major_write_cnt = coro.thread.statics.major_write_cnt + write_cnt

	}
}


// commit may not success
// revert current coro and abort coro behind 
// commit fail just once, the next commit must be success, so we do no need to change the thread_wts
func (coro *Coroutine) Commit() {
	t := coro.txn
	t.Commit()
	coro.thread.statics.commit_cnt = coro.thread.statics.commit_cnt + 1
	MaxCommitIDLock.Lock()
	defer MaxCommitIDLock.Unlock()
	MaxCommitID = t.txn_id
}




