package async

/*
async for no wait 2pl 
*/

import (
	"sync"
	// "t_log"
)


/*
The return of this mutex only focus on the result
if the Lock is being lock stats by the txn then return true no matter it is be lock how much time
So the unlock is no need to return (after use we can sure this mutex must no be lock by this txn!)
*/

type AsyncMutex struct {
	rset *map[int]bool // store the lock map
	w int // write lock's txn_id, -1 is no lock
	lock *sync.Mutex // lock's lock
}

func NewAsyncMutex() *AsyncMutex { 
	return &AsyncMutex{&(map[int]bool{}), -1, &(sync.Mutex{})}
}

/*
If the lock is being rlock by myself then it also can add lock
Fail only when no other txn read and no other txn write
*/
func (am *AsyncMutex) Lock(txn_id int) bool {
	am.lock.Lock()
	defer am.lock.Unlock()
	if ( len(*(am.rset)) == 0 || ( len(*(am.rset)) == 1 && (*(am.rset))[txn_id] == true ) ) && (am.w == txn_id || am.w == -1)  {
		// no other txn read and no other txn write
		am.w = txn_id
		return true
	} else {
		return false
	}
}

func (am *AsyncMutex) Unlock(txn_id int) {
	
	am.lock.Lock()
	// fmt.Printf("%v use UnLock\n", txn_id)
	defer am.lock.Unlock()
	if txn_id == am.w {
		am.w = -1
	}
}


func (am *AsyncMutex) RLock(txn_id int) bool {

	am.lock.Lock()
	defer am.lock.Unlock()
	if am.w == txn_id || am.w == -1 { // no other txn write
		if (*(am.rset))[txn_id] != true {
			(*(am.rset))[txn_id] = true
		}
		return true // add lock success
	} else {
		return false // add lock failed
	}
}



func (am *AsyncMutex) RUnlock(txn_id int) {
	am.lock.Lock()
	defer am.lock.Unlock()
	delete((*(am.rset)), txn_id)
}
