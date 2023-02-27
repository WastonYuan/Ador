package caracal

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
	cw_set := ops.ComplexWriteSet()
	txn := db.NewTXN(txn_id, cw_set)
	n_coro.SetTXN(txn)
	return &n_coro
}

func (coro *Coroutine) Reset() {
	coro.ops.Reset()
}


func (coro *Coroutine) GetTxnId() int {
	return coro.txn.txn_id
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
	// op add scan!!
	if op.Type == t_txn.OP_WRITE {
		coro.txn.Write(op.Key)
		coro.thread.statics.write_cnt = coro.thread.statics.write_cnt + 1
		for i := 0; i < exec_cost; i++ {cc.Sync(coro.thread)}
		coro.ops.Next()
	} else if op.Type == t_txn.OP_READ {
		_, ok := coro.txn.Read(op.Key)
		if ok == true {
			coro.thread.statics.read_cnt = coro.thread.statics.read_cnt + 1
			for i := 0; i < exec_cost; i++ {cc.Sync(coro.thread)}
			coro.ops.Next()
		} else {
			cc.Sync(coro.thread)
			coro.thread.statics.block_step = coro.thread.statics.block_step + 1
		}
	} else { // scan
		t := coro.txn
		base := t.base
		rs := base.Scan(op.Key, op.Len, coro.GetTxnId())
		// scan_s := false
		for e := rs.Front(); e != nil; e = e.Next() {
			cur_key := e.Value.(string)
			for _, ok := coro.txn.Read(cur_key); !ok; _, ok = coro.txn.Read(cur_key) {
				cc.Sync(coro.thread)
				coro.thread.statics.block_step = coro.thread.statics.block_step + 1
			}
			coro.thread.statics.read_cnt = coro.thread.statics.read_cnt + 1
			if e == rs.Front() {
				for i := 0; i < exec_cost; i++ {cc.Sync(coro.thread)}
			} else {
				for i := 0; i < scan_cost / scan_per; i++ {cc.Sync(coro.thread)}
			}
			// if scan_s {
			// 	for i := 0; i < scan_cost; i++ {cc.Sync(coro.thread)}
			// 	scan_s = false
			// } else {
			// 	scan_s = true
			// }
		}
		// if scan_s == true {
		// 	for i := 0; i < scan_cost / scan_per; i++ {cc.Sync(coro.thread)}
		// }
		coro.ops.Next()
	}
	return true

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


func (coro *Coroutine) InstallVersion() bool {

	t := coro.txn
	step, ok := t.Install()
	coro.thread.statics.init_step_cnt = coro.thread.statics.init_step_cnt + step
	return ok
}


