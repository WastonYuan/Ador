package caracal


import (
	"t_index"
	// "t_log"
	"container/list"
	"sync"
)



type BOHM struct {
	// batch_size configure by user
	index *(t_index.Array)
	index_lock *sync.RWMutex
}

func NewBOHM() *BOHM {
	index := t_index.NewArray(2)
	return &(BOHM{index, &sync.RWMutex{}})
}

func (l *BOHM) GetOrInsert(key string, r *Record) *Record {
	l.index_lock.Lock()
	defer l.index_lock.Unlock()
	index := l.index
	return index.GetOrInsert(key, r).(*Record)
}

func (l *BOHM) Search(key string) *Record {
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

/*
TODO
*/
func (l *BOHM) Scan(start_key string, s_len int, txn_id int) *list.List {
	keys := list.New()
	l.index_lock.RLock()
	index := l.index
	key_cnt := 0
	for key, r, ele := index.RangeSearch(start_key); ele != nil && key_cnt < s_len; key, r, ele = index.RangNext(ele) {
		if r.(*Record).IsAvailable(txn_id) {
			keys.PushBack(key)
			key_cnt ++
		}
		
	}
	l.index_lock.RUnlock()

	return keys
}


func (l *BOHM) quickGetOrInsert(key string, r *Record) *Record {
	res := l.Search(key)
	if res == nil {
		res = l.GetOrInsert(key, r)
	}
	return res
}