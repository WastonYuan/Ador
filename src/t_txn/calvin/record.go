package calvin

import (
	"container/list"
	"sync"
	"t_log"
	"fmt"
)

const (
	WRITE_READ_RESERVE = 0
	READ_RESERVE = 1
)

type Ticket struct { // each txn only has one ticket in one record
	reserve_type int // from old to new
	txn_id int
	rwlock *sync.RWMutex
}

func NewTicket(reserve_type int, txn_id int) *Ticket {
	return &Ticket{reserve_type, txn_id, &sync.RWMutex{}}
}


func (t *Ticket) UpdateType(reserve_type int) {
	t.rwlock.Lock()
	defer t.rwlock.Unlock()
	if reserve_type == WRITE_READ_RESERVE {
		t.reserve_type = reserve_type
	}
} 


type Record struct {

	order_lock *list.List // *Ticket
	rwlock *sync.RWMutex
	
}


func (r *Record) GetLockList() string {
	ll := r.order_lock
	var res string = ""
	for ele := ll.Front(); ele != nil; ele = ele.Next() {
		tick := ele.Value.(*Ticket)
		res = res + fmt.Sprintf("%v ", tick.txn_id)
	}
	return res
}

func NewRecord() *Record {
	return &Record{list.New(), &sync.RWMutex{}}
}



func (r *Record) GetWriteLock(txn_id int) (int, bool) {
	r.rwlock.RLock()
	defer r.rwlock.RUnlock()

	rl := r.order_lock


	// dealing with the first lock
	ele := rl.Front()
	t := ele.Value.(*Ticket)

	step := 1
	if t.txn_id == txn_id { // this must be the read_write_reserve!
		return step, true
	} else {
		// t_log.Log(t_log.DEBUG, "txn %v get write lock fail: ll: %v\n", txn_id, r.GetLockList())
		return step, false
	}
}

func (r *Record) GetReadLock(txn_id int) (int, bool) {
	r.rwlock.RLock()
	defer r.rwlock.RUnlock()

	rl := r.order_lock

	ele := rl.Front()
	t := ele.Value.(*Ticket)

	step := 1
	if t.txn_id == txn_id { // no matter the type is read or write the lock can be granted if first is own
		return step, true
	} else if t.reserve_type == WRITE_READ_RESERVE { // if first is other's write then return false
		// t_log.Log(t_log.DEBUG, "txn %v get read lock fail ll: %v\n", txn_id, r.GetLockList())
		return step, false
	}

	for ele = ele.Next(); ele != nil; ele = ele.Next() { // from second to end
		step ++
		t := ele.Value.(*Ticket) 
		if t.reserve_type == WRITE_READ_RESERVE { // loop until the first write
			// t_log.Log(t_log.DEBUG, "txn %v get read lock fail ll: %v\n", txn_id, r.GetLockList())
			return step, false
		} else {
			if t.txn_id == txn_id {
				return step, true
			}
		}
	}

	t_log.Log(t_log.ERROR, "error point in get read lock\n")

	return step, false
}


func (r *Record) Reserve(txn_id int, reserve_type int) (int, *list.Element, bool) {
	
	r.rwlock.Lock()
	defer r.rwlock.Unlock()

	lock_l := r.order_lock

	
	step := 0
	var ele *list.Element = nil
	var res_ele *list.Element = nil
	for ele = lock_l.Front(); ele != nil; ele = ele.Next() {
		step ++
		t := ele.Value.(*Ticket)
		if t.txn_id < txn_id {
			continue
		} else if t.txn_id == txn_id {
			t.UpdateType(reserve_type) // the logic in the function, update write can trigger update
			return step, ele, true
		} else {
			n_ticket := NewTicket(reserve_type, txn_id)
			res_ele = lock_l.InsertBefore(n_ticket, ele)
			return step, res_ele, true
		}
	}

	if ele == nil { // the reserve txn_id is max (or the reserve list is empty)
		step ++
		n_ticket := NewTicket(reserve_type, txn_id)
		res_ele = lock_l.PushBack(n_ticket)
	}

	return step, res_ele, true

}


func (r *Record) ReleaseLock(ele *list.Element) (int, bool) {
	r.rwlock.Lock()
	defer r.rwlock.Unlock()

	rl := r.order_lock
	rl.Remove(ele)
	return 1, true
}

