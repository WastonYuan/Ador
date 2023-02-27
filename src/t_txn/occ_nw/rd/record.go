package rd

// import "fmt"
import (
	"t_txn/sstpl_nw/async"
	"sync"
	"t_log"
)


type Record struct {
	wts int
	rwlock *async.AsyncMutex
	wlock *sync.Mutex
}

func NewRecord() *Record {
	return &Record{-1, async.NewAsyncMutex(), &sync.Mutex{}}
}

/*
samphore for control the core count
txn_id for add lock
*/
func (r *Record) Read(txn_id int) bool {
	
	r.rwlock.RLock(txn_id)
	defer r.rwlock.RUnlock(txn_id)
	// validate
	if r.wts <= txn_id {
		
		return true

	} else {
		t_log.Log(t_log.DEBUG, "wts:%v, commit_id:%v\n", r.wts, txn_id)
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


func (r *Record) Commit(txn_id int) {
	r.wlock.Lock()
	defer r.wlock.Unlock()
	r.wts = txn_id
	r.DoneWrite(txn_id)
}