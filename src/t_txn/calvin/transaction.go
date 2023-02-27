package calvin

import (
	// "t_index"
	// "t_log"
	"container/list"
	// "t_txn"
)




type TXN struct {
	txn_id int
	reserve *(map[*list.Element](*Record)) // for commit (gc)
	write_set *(map[string]bool)
	read_set *(map[string]bool)
	key_r_cache *(map[string](*Record))
	base *Calvin
}


func (calvin *Calvin) NewTXN(txn_id int, write_set *(map[string]bool), read_set *(map[string]bool)) *TXN {
	return &TXN{txn_id, &map[*list.Element](*Record){}, write_set, read_set, &map[string](*Record){}, calvin}
}



/*
write always ok
*/


func (t *TXN) quickGetOrInsert(key string, n_r *Record) *Record {
	key_r := t.key_r_cache
	base := t.base
	r, ok := (*key_r)[key]
	if ok == false {
		r = base.quickGetOrInsert(key, n_r)
		(*key_r)[key] = r
	}
	return r
}

func (t *TXN) quickSearch(key string) *Record {
	key_r := t.key_r_cache
	base := t.base
	r, ok := (*key_r)[key]
	if ok == false {
		r = base.Search(key)
		if r != nil {
			(*key_r)[key] = r
		}
	}
	return r
}


func (t *TXN) Reserve() (int, bool) {
	w_s := t.write_set
	step := 0
	for key, _ := range *w_s {
		r := t.quickGetOrInsert(key, NewRecord())
		s, ele, _ := r.Reserve(t.txn_id, WRITE_READ_RESERVE)
		(*t.reserve)[ele] = r
		step = step + s
	}

	r_s := t.read_set
	for key, _ := range *r_s {
		r := t.quickGetOrInsert(key, NewRecord())
		s, ele, _ := r.Reserve(t.txn_id, READ_RESERVE)
		(*t.reserve)[ele] = r
		step = step + s
	}
	return step, true
}


func (t *TXN) GetLock() (int, bool) {
	w_s := t.write_set

	step := 0
	for key, _ := range *w_s {
		r := t.quickGetOrInsert(key, NewRecord())
		s, ok := r.GetWriteLock(t.txn_id)
		step += s
		if ok == false {
			return step, false
		}
	}

	r_s := t.read_set
	for key, _ := range *r_s {

		r := t.quickGetOrInsert(key, NewRecord())
		s, ok := r.GetReadLock(t.txn_id)
		step += s
		if ok == false {
			return step, false
		}
	}
	return step, true 
}

func (t *TXN) Write(key string) (int, bool) {
	return 1, true
}


func (t *TXN) Read(key string) (int, bool) {
	return 1, true
}


/*
Release the lock
*/
func (t *TXN) Commit() (int, bool) {
	reserves := t.reserve
	step := 0
	for ele, r := range *reserves {
		step ++
		r.ReleaseLock(ele)
	}
	return step, true
}