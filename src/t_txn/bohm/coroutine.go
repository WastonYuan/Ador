package bohm

import (
	"t_txn"
	// "t_log"
)


// the coroutine mainly use for statistics



type Coroutine struct {
	ops t_txn.AccessPtr
	txn *TXN
	thread *Thread
}


func NewCoroutine(ops t_txn.AccessPtr, txn_id int, db *BOHM, thread *Thread) *Coroutine {
	n_coro := Coroutine{ops, nil, thread}
	_, w_set := ops.ReadWriteSet()
	txn := db.NewTXN(txn_id, w_set)
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

func (coro *Coroutine) Execute() bool {

	op := coro.ops.Get()
	if op == nil {
		return false
	}
	if op.Type == t_txn.OP_WRITE {
		coro.txn.Write(op.Key)
		// write never be false
		coro.thread.statics.write_cnt = coro.thread.statics.write_cnt + 1
		for i:=0; i < exec_cost; i++ {cc.Sync(coro.thread)}
		coro.ops.Next()
		return true
		
	} else {
		_, ok := coro.txn.Read(op.Key)
		if ok {
			for i:=0; i < exec_cost; i++ {cc.Sync(coro.thread)}
			coro.thread.statics.read_cnt = coro.thread.statics.read_cnt + 1
			coro.ops.Next()
			return true
		}
		coro.thread.statics.block_step = coro.thread.statics.block_step + 1
		cc.Sync(coro.thread)
		return true
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


func (coro *Coroutine) InstallVersion() (int, bool) {

	t := coro.txn
	step, ok := t.Install()
	return step, ok
}


