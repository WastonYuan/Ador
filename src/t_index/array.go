package t_index

import (
	// "fmt"
	// "t_txn/tpl/rd"
	// "t_txn/calvin/rd"
	"sync"
	"container/list"
	// "t_log"
)

/* type alias for configure transaction type */
// record is *rd.Record


/*
Mmap means multi-map not the memory-map
*/
type Array struct { // read only class
	
	l *list.List
	hash_index *(map[string](*list.Element))
	lock *sync.RWMutex
}


type Pair struct {
	key string
	value record
}

func NewArray(m int) *Array {
	arr := Array{list.New(), &map[string](*list.Element){}, &(sync.RWMutex{})}
	return &arr
} 

// if exist return the interface of *Record
// else return nil
// use hash index
func (arr *Array) Search(key string) record {

	arr.lock.RLock()
	defer arr.lock.RUnlock()
	e, ok := (*arr.hash_index)[key]
	if ok {
		pair := e.Value.(*Pair)
		return pair.value
	}
	return nil
} 

/* be ware of using in concurency */
func (arr *Array) Insert(key string, r record) {

	l := arr.l
	arr.lock.Lock()
	defer arr.lock.Unlock()
	for e := l.Front(); e != nil; e = e.Next() {
		// do something with e.Value
		pair := e.Value.(*Pair)
		if pair.key < key {
			continue
		} else if pair.key == key { // already exist
			pair.value = r
			return
		} else { // there a new pair
			np := Pair{key, r}
			n_e := l.InsertBefore(&np, e)
			(*arr.hash_index)[key] = n_e
			return
		}
	}
	np := Pair{key, r}
	l.PushBack(&np)
}

func (arr *Array) ReNew() *Array {
	return NewArray(1)
}

/*
GetOrInsert(key)
if the key is in the index then return
if not then insert to the index and return the get value
all run in atomic
this should be run before Search since it need to lock that may occur conflict

with this the same key will not occur multi dealing record!
*/

func (arr *Array) GetOrInsert(key string, r record) record {

	l := arr.l
	arr.lock.Lock()
	defer arr.lock.Unlock()

	for e := l.Front(); e != nil; e = e.Next() {
		// do something with e.Value
		pair := e.Value.(*Pair)
		if pair.key < key {
			continue
		} else if pair.key == key { // already exist then get
			return pair.value
		} else { // not exist then insert
			np := Pair{key, r}
			n_e := l.InsertBefore(&np, e)
			(*arr.hash_index)[key] = n_e
			return np.value
		}
	}
	np := Pair{key, r}
	n_e := l.PushBack(&np)
	(*arr.hash_index)[key] = n_e
	return np.value
}


func (arr *Array) All() *list.List {
	l := arr.l
	arr.lock.RLock()
	defer arr.lock.RUnlock()
	res := list.New()
	for ele := l.Front(); ele != nil; ele = ele.Next() {
		r := ele.Value.(*Pair).value
		res.PushBack(r)
	}
	return res
}

// can not use nil so the third parameter is the success or not tag
func (arr *Array) RangeSearch(key string) (string, record, *list.Element) {

	l := arr.l
	arr.lock.RLock()
	defer arr.lock.RUnlock()

	e := l.Front()
	for ; e != nil; e = e.Next() {
		pair := e.Value.(*Pair)
		if pair.key < key {
			continue
		} else {
			break
		}
	} 
	if e != nil {
		return e.Value.(*Pair).key, e.Value.(*Pair).value, e
	}
	return "", nil, e
}

func (arr *Array) RangNext(range_cur *list.Element) (string, record, *list.Element) {
	e := range_cur.Next()
	if e != nil {
		return e.Value.(*Pair).key, e.Value.(*Pair).value, e
	}
	return "", nil, e

}


func (arr *Array) KeyPrev(key string) (string, record, bool) {
	hash := arr.hash_index
	ele := (*hash)[key]
	prev := ele.Prev()
	if prev != nil {
		return prev.Value.(*Pair).key, prev.Value.(*Pair).value, true
	} else {
		return "", nil, false
	}
}


func (arr *Array) KeyNext(key string) (string, record, bool) {
	hash := arr.hash_index
	ele := (*hash)[key]
	next := ele.Next()
	if next != nil {
		return next.Value.(*Pair).key, next.Value.(*Pair).value, true
	} else {
		return "", nil, false
	}
}