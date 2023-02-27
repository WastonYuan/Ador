package rd

import (
	"sync"
)

const (
	PENDING int = 0
	COMMITED 	= 1
	ABORT		= 2
)


type Version struct {
	wts int // wts not change once set
	rts int // when uncommited rts always equal to wts
	stats int
	rwlock *sync.RWMutex
}

func (v *Version) GetStats() int {
	return v.stats
}


func (v *Version) Validate(txn_id int) bool {
	v.rwlock.RLock()
	defer v.rwlock.RUnlock()
	if v.wts < txn_id && v.rts > txn_id {
		return false
	} else {
		return true
	}
}

func (v *Version) Read(txn_id int) bool {
	v.rwlock.RLock()
	defer v.rwlock.RUnlock()
	if v.stats == ABORT {
		return false
	}
	if v.wts < txn_id && v.stats == COMMITED || v.wts == txn_id {
		v.rts = txn_id
		return true
	} else {
		return false
	}
}


func (v *Version) Commit() {
	v.rwlock.Lock()
	defer v.rwlock.Unlock()
	v.stats = COMMITED	
}

func (v *Version) Abort() {
	v.rwlock.Lock()
	defer v.rwlock.Unlock()
	v.stats = ABORT	
}

func NewVersion(wts int) *Version {
	return &Version{wts, wts, PENDING, &(sync.RWMutex{})}
}






