package gria

import (
	"sync"
	"container/list"
	// "t_txn"
	// "t_log"
	"fmt"
)




// by other txn
// R Then W R Then R is allowed
// W Then R W Then W? is not allowed


type Record struct {
	vl *(list.List) // *Version
	rwlock *(sync.RWMutex)
}


func NewRecord() *Record {
	l := list.New()
	t := TXN{-1, -1, -1, nil, nil, nil, false, COMMIT ,nil}
	nv := NewVersion(&t)
	l.PushBack(nv)

	return &(Record{l, &(sync.RWMutex{})})
}

/*
Print list with commit_id and group_id
*/
func (r *Record) PrintList() string {
	r.rwlock.RLock()
	r.rwlock.RUnlock()
	var s string
	for e := r.vl.Front(); e != nil; e = e.Next() {
		v := e.Value.(*Version)
		s = s + fmt.Sprintf("[%d|%d] ", v.owner_txn.commit_id, v.owner_txn.group_id)
	}
	return s
}



/*
write must append
*/
func (r *Record) Write(txn *TXN) *(list.Element) {
	r.rwlock.Lock()
	defer r.rwlock.Unlock()
	nv := NewVersion(txn)
	var ele *(list.Element) = nil
	for e := r.vl.Front(); e != nil; e = e.Next() {
		v := e.Value.(*Version)
		if txn.commit_id >= v.owner_txn.commit_id {
			continue
		} else {
			ele = r.vl.InsertBefore(nv, e)
			break
		}
	}
	if ele == nil {
		ele = r.vl.PushBack(nv)
	}
	return ele
}


func (r *Record) Read(txn *TXN) *(list.Element) {
	r.rwlock.RLock()
	r.rwlock.RUnlock()
	var pre_v *(list.Element) = nil
	for e := r.vl.Front(); e != nil; e = e.Next() {
		v := e.Value.(*Version)
		if v.owner_txn.group_id == txn.group_id || v.owner_txn.group_id == -1 { // only read the snapshot or own group write
			if v.owner_txn.commit_id <= txn.commit_id {
				pre_v = e
			} else { // impossible run to this, because the read one must be the last of the same group in this version link
				break
			}
		}
	}
	// save the max read txn for reorder
	v := pre_v.Value.(*Version)
	if v.max_read_txn == nil || v.max_read_txn.commit_id < txn.commit_id {
		// t_log.Log(t_log.INFO, "set version max read, before %v: now: %v\n", v.max_read_txn, txn)
		v.max_read_txn = txn
	}
	return pre_v
}


func (r *Record) ReadVersion(txn *TXN) *Version {
	return r.Read(txn).Value.(*Version)
}


/* 
read itself not need to validation 
if return nil means validation pass
else if return ele means the version should read
*/
func (r *Record) ReadValidation(txn *TXN, ele *(list.Element)) *(list.Element) {
	v := ele.Value.(*Version)
	if v.owner_txn.commit_id == txn.commit_id {
		return nil
	}
	for e := ele.Next(); e != nil; e = e.Next() {
		behind_v := e.Value.(*Version)
		// v := e.Value.(*Version)
		// if the same group the behind write commit_id can not smaller than read commit_id
		// so if the next version > commit_id must be other group
		if behind_v.owner_txn.group_id != txn.group_id {
			if behind_v.owner_txn.commit_id < txn.commit_id {
				return e
			} else {
				return nil
			}
		}
	}
	return nil
}
