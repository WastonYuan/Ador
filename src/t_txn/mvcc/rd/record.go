package rd

// import "fmt"
import (
	"container/list"
	"sync"
)
/* 2PL record and operation */




type Record struct {
	vlist *list.List // for save *Version
	rwlock *sync.RWMutex
}


func NewRecord() *Record {
	return &Record{list.New(), &(sync.RWMutex{})}
}





func (r *Record) Write(txn_id int) *Version {
	r.rwlock.Lock()
	defer r.rwlock.Unlock()
	var cur_ele *list.Element
	nv := NewVersion(txn_id)
	for cur_ele = r.vlist.Front(); cur_ele != nil; cur_ele = cur_ele.Next() {
		v := cur_ele.Value.(*Version)
		if v.wts > txn_id {
			break
		} else {
			if v.Validate(txn_id) == false {
				return nil
			} else {
				continue
			}
		}
	}
	if cur_ele == nil {
		r.vlist.PushBack(nv)
	} else {
		r.vlist.InsertBefore(nv, cur_ele)
	}
	
	return nv

}


func (r *Record) Read(txn_id int) bool {
	r.rwlock.RLock()
	defer r.rwlock.RUnlock()
	var cur_ele *list.Element = nil
	var pre_ele *list.Element = nil
	
	for cur_ele = r.vlist.Front(); cur_ele != nil; cur_ele = cur_ele.Next() {
		v := cur_ele.Value.(*Version)
		if v.GetStats() == ABORT { // ignore abort
			continue
		}
		if v.wts > txn_id {
			break
		}
		pre_ele = cur_ele
	}
	if pre_ele == nil {
		return true
	} else {
		v := pre_ele.Value.(*Version)
		return v.Read(txn_id)
	}
}