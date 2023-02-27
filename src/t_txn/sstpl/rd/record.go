package rd

// import "fmt"
import "t_txn/sstpl_nw/async"

/* 2PL record and operation */

type Record struct {
	rwlock *async.AsyncMutex
}

func NewRecord() *Record {
	return &Record{async.NewAsyncMutex()}
}

/*
samphore for control the core count
txn_id for add lock
*/
func (r *Record) Read(txn_id int) bool {
	
	if r.rwlock.RLock(txn_id) {
		return true
	} else {
		return false
	}
}


func (r *Record) Write(txn_id int) bool {
	
	if r.rwlock.Lock(txn_id) {
		return true
	} else {
		return false
	}
	
}


func (r *Record) DoneWrite(txn_id int) {
	r.rwlock.Unlock(txn_id)
}


func (r *Record) DoneRead(txn_id int) {
	r.rwlock.RUnlock(txn_id)
}