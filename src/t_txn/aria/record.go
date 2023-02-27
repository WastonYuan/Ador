package aria

import (
	"sync"
	// "container/list"
	// "t_txn"
)




// by other txn
// R Then W R Then R is allowed
// W Then R W Then W? is not allowed

type Record struct {
	min_wid int // init with -1 means no be writed yet
	min_rts int
	rwlock *(sync.RWMutex)
}

func (r *Record) Reset() {
	r.rwlock.Lock()
	defer r.rwlock.Unlock()
	r.min_wid = -1
}

func (r *Record) Get_min_wid() int {
	r.rwlock.RLock()
	defer r.rwlock.RUnlock()
	return (r.min_wid)
}

func (r *Record) Get_min_rts() int {
	r.rwlock.RLock()
	defer r.rwlock.RUnlock()
	return (r.min_rts)
}

func NewRecord() *Record {
	return &(Record{-1, -1, &(sync.RWMutex{})})
}

/*
Write/Read in first phase (execution phase)
this may be interupt when exec
*/


func (r *Record) Write(txn_id int) {
	r.rwlock.Lock()
	defer r.rwlock.Unlock()
	if r.min_wid == -1 { // not write this record yet
		r.min_wid = txn_id
	}

	if txn_id < r.min_wid  { // write after operation will all be abort
		r.min_wid = txn_id
	}
}

func (r *Record) Read(txn_id int) {
	r.rwlock.Lock()
	defer r.rwlock.Unlock()
	// add to w_txns in order
	if txn_id < r.min_rts {
		r.min_rts = txn_id
	}
}
