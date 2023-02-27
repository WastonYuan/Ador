package caracal

import (
	"container/list"
	"sync"
	// "t_log"
	// "fmt"
)

type Record struct {
	vl *(list.List) // with version from old to new
	rwlock *(sync.RWMutex) // the read lock no must use since only first phase write but not read
}

func NewRecord() *Record {
	l := list.New()
	return &( Record{l, &(sync.RWMutex{})})
}

/*
The Write operation in the first phase
this different from the mvcc
mvcc need validate the read txn_id is larger than this txn_id
BOHM do not need to worry (validate) since write only occur in first phase which contained read operation
Write: new a version and insert to right position
Only write will change the version List (read change version)
*/
func (r *Record) Install(txn_id int, count int) (int, *Version, bool) {
	r.rwlock.Lock()
	defer r.rwlock.Unlock()
	nv := NewVersion(txn_id, count, PENDING)
	step := 0
	var e *(list.Element)
	for e = r.vl.Front(); e != nil; e = e.Next() {
		step ++
		// do something with e.Value
		v := e.Value.(*Version)
		if !v.IsVisible(txn_id) {
			break
		}
	}
	if e == nil {
		r.vl.PushBack(nv)
	} else {
		r.vl.InsertBefore(nv, e)
	}
	return step, nv, true
}

func (r *Record) VersionListString() string {
	r.rwlock.RLock()
	defer r.rwlock.RUnlock()
	var res string
	for e := r.vl.Front(); e != nil; e = e.Next() {
		v := e.Value.(*Version)
		res = res + v.GetString()
	}
	return res
} 


/*
it may return false depend on the write is ok
if read suceess version != nil
if bool == true means need fetch a COMMIT read record
*/
func (r *Record) Read(txn_id int) (int, bool) {
	r.rwlock.RLock()
	defer r.rwlock.RUnlock()
	step := 1
	var e *(list.Element) = nil
	var pre *(list.Element) = nil
	for e = r.vl.Front(); e != nil; e = e.Next() {
		step ++
		v := e.Value.(*Version)
		if v.IsOwnVersion(txn_id) { // first deal with own version, if can not read then read pre
			if v.GetStats() == MODIFIED { // impossible commit
				return step, true
			} else { // PENDING then read prev
				break
			}
		}
		if v.IsVisible(txn_id) { // visible but not own version
			pre = e
			continue
		} else { // not visible then read prev
			break
		}
	}
	// read pre
	if pre == nil { // the first version is invisible
		// read nothing
		return step, true
	} else {
		r_v := pre.Value.(*Version)
		if r_v.GetStats() == COMMITED {
			return step, true 
		} else {
			return step, false // PENDING OR MODIFIED
		}
	}
}


func (r *Record) IsAvailable(txn_id int) bool {
	r.rwlock.RLock()
	defer r.rwlock.RUnlock()
	front_ele := r.vl.Front()
	front_v := front_ele.Value.(*Version)
	if front_v.IsVisible(txn_id) {
		return true
	} else {
		return false
	}
}