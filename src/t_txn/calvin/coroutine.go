package calvin

import (
	"t_txn"
	// "t_log"
)



// the coroutine mainly use for statistics
// Coroutine function do no need the return (blocking is in this level)


type Coroutine struct {
	ops t_txn.AccessPtr
	txn *TXN
	thread *Thread
}

func (coro *Coroutine) GetTxnId() int {
	return coro.txn.txn_id
}


func NewCoroutine(ops t_txn.AccessPtr, txn_id int, db *Calvin, thread *Thread) *Coroutine {
	n_coro := Coroutine{ops, nil, thread}
	r_set, w_set := ops.ReadWriteSet()
	txn := db.NewTXN(txn_id, w_set, r_set)
	n_coro.SetTXN(txn)
	return &n_coro
}

func (coro *Coroutine) Reset() {
	coro.ops.Reset()
}


func (coro *Coroutine) SetThread(t *Thread) {
	coro.thread = t
}

func (coro *Coroutine) SetTXN(txn *TXN) {
	coro.txn = txn 
}


func (coro *Coroutine) Reserve() {
	txn := coro.txn
	step, _ := txn.Reserve()
	coro.thread.statics.reserve_step = coro.thread.statics.reserve_step + step
}

func (coro *Coroutine) IsRunable() bool {

	txn := coro.txn
	step := 0

	s, ok := txn.GetLock()
	step += s
	if ok {
		coro.thread.statics.lock_step = coro.thread.statics.lock_step + step
		return true
	} else {
		// t_log.Log(t_log.DEBUG, "get lock failed\n")
		coro.thread.statics.lock_step = coro.thread.statics.lock_step + step
		coro.thread.statics.block_step = coro.thread.statics.block_step + 1
		return false
	}
}

func (coro *Coroutine) Execute() bool {
	
	op := coro.ops.Get()
	if op == nil {
		return false
	}
	// op add scan!!
	if op.Type == t_txn.OP_WRITE {
		coro.Write(op.Key)
	} else if op.Type == t_txn.OP_READ {
		coro.Read(op.Key)
	}
	coro.ops.Next()
	return true
}



// Read may block until read ok
func (coro *Coroutine) Read(key string) {
	// t_log.Log(t_log.DEBUG, "read\n")
	_, ok := coro.txn.Read(key)
	if ok == true {
		coro.thread.statics.read_cnt = coro.thread.statics.read_cnt + 1
	}
} 

// Write must success
func (coro *Coroutine) Write(key string) {
	_, ok := coro.txn.Write(key)
	// write never be false
	if ok == true {
		coro.thread.statics.write_cnt = coro.thread.statics.write_cnt + 1
	}
}


func (coro *Coroutine) Commit() bool {
	t := coro.txn
	step, ok := t.Commit()
	coro.thread.statics.commit_step = coro.thread.statics.commit_step + step
	if ok {
		coro.thread.statics.commit_cnt = coro.thread.statics.commit_cnt + 1
	}
	return ok
}


