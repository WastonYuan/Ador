package bohm

import (
	"sync"
	"fmt"
	"t_log"
)


/*
Version Stats
*/
const (
	PENDING int = 0
	MODIFIED = 1 // use for reading own version
	COMMITED = 2
	
)


type Version struct {
	wts int // wts not change once set
	// rts int // save the max read txn the rts can not be revert !!!
	// this use for garbage collection in toro
	// rts must >= wts
	stats int
	rwlock *sync.RWMutex
}

func (v *Version) GetStats() int {
	v.rwlock.RLock()
	defer v.rwlock.RUnlock()
	return v.stats
}

func NewVersion(wts int, stats int) *Version {
	return &Version{wts, stats, &(sync.RWMutex{})}
}

func (v *Version) UpdateStats(stats int) {
	v.rwlock.Lock()
	defer v.rwlock.Unlock()
	v.stats = stats
}

/*
all visiable version will return true
so the record gurantee the read order (old to new)
*/
func (v *Version) IsVisible(txn_id int) bool {
	v.rwlock.RLock()
	defer v.rwlock.RUnlock()
	// validate
	// this version write by other txn or write by this txn
	// write by other txn condition can be adapt to this txn
	if v.wts <= txn_id {
		// v.rts = txn_id
		return true
	} else {
		return false
	}
}

func (v *Version) IsOwnVersion(txn_id int) bool {
	v.rwlock.RLock()
	defer v.rwlock.RUnlock()

	if v.wts == txn_id {
		return true
	} else {
		return false
	}
}




func (v1 *Version) GetString() string {
	return fmt.Sprintf("[%v %v]", v1.wts, v1.stats)
}

func (v *Version) Commit() (int, bool) {
	v.rwlock.Lock()
	defer v.rwlock.Unlock()
	v.stats = COMMITED
	return 1, true
}


func (v *Version) Write() (int, bool) {
	v.rwlock.Lock()
	defer v.rwlock.Unlock()
	if v.stats == COMMITED {
		t_log.Log(t_log.ERROR, "error write commit\n")
	}
	v.stats = MODIFIED
	return 1, true
}