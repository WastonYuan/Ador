package aria

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


func NewCoroutine(ops t_txn.AccessPtr, txn_id int, db *Aria, thread *Thread) *Coroutine {
	n_coro := Coroutine{ops, nil, thread}
	txn := db.NewTXN(txn_id)
	n_coro.SetTXN(txn)
	return &n_coro
}

func (coro *Coroutine) SetThread(t *Thread) {
	coro.thread = t
}

func (coro *Coroutine) Reset() {
	coro.ops.Reset()
	coro.txn.Reset()
}

func (coro *Coroutine) SetTXN(txn *TXN) {
	coro.txn = txn 
}

func (coro *Coroutine) Execution() bool {
	for true {
		op := coro.ops.Get()
		if op == nil {
			break
		}
		// op add scan!!
		if op.Type == t_txn.OP_WRITE {
			coro.Write(op.Key)
		} else if op.Type == t_txn.OP_READ {
			coro.Read(op.Key) // this may loop
		}
		coro.ops.Next()
	}
	return true
}



// Read may block until read ok
func (coro *Coroutine) Read(key string) bool {
	for true {
		ok := coro.txn.Read(key)
		if ok == true {
			coro.thread.statics.read_cnt = coro.thread.statics.read_cnt + 1
			break
		}
	}
	return true
} 

// Write must success
func (coro *Coroutine) Write(key string) {
	ok := coro.txn.Write(key)
	// write never be false
	if ok == true {
		coro.thread.statics.write_cnt = coro.thread.statics.write_cnt + 1
	}
}



func (coro *Coroutine) Commit() bool {
	t := coro.txn
	ok := t.Commit()
	if ok {
		coro.thread.statics.commit_cnt = coro.thread.statics.commit_cnt + 1
	} else {
		t.Reset()
		coro.thread.statics.abort_cnt = coro.thread.statics.abort_cnt + 1
	}
	return ok
}





