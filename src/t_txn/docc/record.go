package docc

import (
	"sync"
	// "container/list"
	// // "t_txn"
	// "t_log"
	// "fmt"
)


type Record struct {
	wts int // commit tid
	rwlock *(sync.RWMutex)
}


func NewRecord() *Record {
	return &(Record{-1, &sync.RWMutex{}})
}


/*
write must ok
*/
func (r *Record) Write(txn *TXN) {
	
}


/*
read must ok
*/
func (r *Record) Read(txn *TXN) int {
	r.rwlock.RLock()
	defer r.rwlock.RUnlock()

	return r.wts

}


func (r *Record) Commit(txn *TXN) {
	r.rwlock.Lock()
	defer r.rwlock.Unlock()
	// validate 
	r.wts = txn.txn_id
}



func (r *Record) Abort(txn *TXN) {

}


func (r *Record) Validate(rts int) bool {
	r.rwlock.RLock()
	defer r.rwlock.RUnlock()
	return r.wts == rts

}