package sgpm

import (
	"container/list"
	// "t_txn"
	"sync"
)

/*
use for manager the opss
control the ops assign to coro one by one
*/

type GlobalQueue struct {
	coros *list.List // elemet is *coro
	IsEmpty bool
	size int
	bm *BatchManager
	rwlock *sync.RWMutex
}


func (gq *GlobalQueue) Len() int {
	return gq.coros.Len()
}


func (gq *GlobalQueue) GetSize() int {
	return gq.size
}
/*
this run in ourside
return  is sucess or not
*/
func (b *GlobalQueue) Insert(coro *Coroutine) bool {
	b.rwlock.Lock()
	defer b.rwlock.Unlock()
	b.IsEmpty = false
	if b.coros.Len() > b.size {
		return false
	} else {
		b.coros.PushBack(coro)
		return true
	}
}


func (b *GlobalQueue) SwapFront(coro *Coroutine) *Coroutine {
	b.rwlock.Lock()
	defer b.rwlock.Unlock()
	b.IsEmpty = false

	f_ele := b.coros.Front()
	if f_ele == nil {
		return coro
	}
	f_coro := f_ele.Value.(*Coroutine)
	b.coros.Remove(f_ele)

	b.coros.PushFront(coro)
	return f_coro
}



func (b *GlobalQueue) InsertLast(coro *Coroutine) {
	b.rwlock.Lock()
	defer b.rwlock.Unlock()
	b.IsEmpty = false

	b.coros.PushBack(coro)

}