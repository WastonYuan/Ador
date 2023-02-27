package aria

import (
	"t_index"
	"sync"
	"container/list"
)

type Aria struct {
	// batch_size configure by user
	index *(t_index.Array)
	index_lock *sync.RWMutex

}


func (aria *Aria) Reset() {
	all_l := aria.index.All()
	for ele := all_l.Front(); ele != nil; ele = ele.Next() {
		r := ele.Value.(*Record)
		r.Reset()
	}

}


func NewAria() *Aria {
	index := t_index.NewArray(2)
	return &(Aria{index, &sync.RWMutex{}})
}


func (l *Aria) GetOrInsert(key string, r *Record) *Record {
	l.index_lock.Lock()
	defer l.index_lock.Unlock()
	index := l.index
	return index.GetOrInsert(key, r).(*Record)
}

func (l *Aria) Search(key string) *Record {
	l.index_lock.RLock()
	defer l.index_lock.RUnlock()
	index := l.index
	r := index.Search(key)
	if r == nil {
		return nil
	} else {
		return r.(*Record)
	}
}


func (l *Aria) quickGetOrInsert(key string, r *Record) *Record {
	res := l.Search(key)
	if res == nil {
		res = l.GetOrInsert(key, r)
	}
	return res
}

func (aria *Aria) Scan(start_key string, loop int) *list.List {
	// aria.index_lock.RLock()
	// // index := aria.index
	// //  := index.RangeSearch(start_key)


	// aria.index_lock.RUnlock()

	return nil
}
