

package ador_coro


import (
	"t_index"
	"sync"
	"container/list"
	// "t_log"
)


type Ody struct {

	index *(t_index.Array)

	index_lock *sync.RWMutex

	rq_lock *sync.RWMutex

	max_commit_id int

	commit_id_lock *sync.RWMutex
}


func NewOdy() *Ody {
	index := t_index.NewArray(2)
	return &Ody{index, &sync.RWMutex{}, &sync.RWMutex{}, 0, &sync.RWMutex{}}
}


// return the key and record in the range
// just return the available records, since the record avaialble or not depending on the txn's thread id, 
// RangeSearch need the TXN as one of the parameters. 
func (l *Ody) RangeSearch(start_key string) (string, *Record, *list.Element) {
	l.index_lock.RLock() 
	defer l.index_lock.RUnlock()
	index := l.index
	
	key, rd, ele := index.RangeSearch(start_key)
	
	if ele != nil {
		return key, rd.(*Record), ele
	} else {
		return "", nil, nil
	}
}

func (l *Ody) RangNext(ele *list.Element) (string, *Record, *list.Element) {
	l.index_lock.RLock()
	defer l.index_lock.RUnlock()
	index := l.index
	key, rd, ele := index.RangNext(ele)
	if ele != nil {
		return key, rd.(*Record), ele
	} else {
		return "", nil, nil
	}
}



func (l *Ody) GetOrInsert(key string, r *Record) *Record {
	l.index_lock.Lock()
	defer l.index_lock.Unlock()
	index := l.index
	return index.GetOrInsert(key, r).(*Record)
}

func (l *Ody) Search(key string) *Record {
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


func (l *Ody) quickGetOrInsert(key string, r *Record) *Record {
	res := l.Search(key)
	if res == nil {
		res = l.GetOrInsert(key, r)
	}
	return res
}

func (o *Ody) GetPrevKey(key string) (string, *Record, bool) {
	index := o.index
	o.index_lock.RLock()
	defer o.index_lock.RUnlock()
	key, r, ok := index.KeyPrev(key)
	if ok == true {
		return key, r.(*Record), ok
	} else {
		return "", nil, false
	}
	
}

