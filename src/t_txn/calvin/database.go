package calvin


import (
	"t_index"
	// "t_log"
	"sync"
)



type Calvin struct {
	// batch_size configure by user
	index *(t_index.Array)
	index_lock *sync.RWMutex
}

func NewCalvin() *Calvin {
	index := t_index.NewArray(2)
	return &(Calvin{index, &sync.RWMutex{}})
}

func (l *Calvin) GetOrInsert(key string, r *Record) *Record {
	l.index_lock.Lock()
	defer l.index_lock.Unlock()
	index := l.index
	return index.GetOrInsert(key, r).(*Record)
}

func (l *Calvin) Search(key string) *Record {
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


func (l *Calvin) quickGetOrInsert(key string, r *Record) *Record {
	res := l.Search(key)
	if res == nil {
		res = l.GetOrInsert(key, r)
	}
	return res
}