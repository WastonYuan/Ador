package caracal

import (
	// "t_index"
	"t_log"
	// "t_txn"
)




type TXN struct {
	txn_id int
	c_write_set *(map[string]int)
	write_version *(map[string](*Version)) // init when preparation phase
	key_r_cache *(map[string](*Record))
	base *BOHM
}


func (bohm *BOHM) NewTXN(txn_id int, write_set *(map[string]int)) *TXN {
	txn := TXN{txn_id, write_set, &(map[string](*Version){}), &map[string](*Record){}, bohm}
	return &txn
}



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

/*
write always ok
*/
func (t *TXN) Write(key string) (int, bool) {
	wv := t.write_version
	r := t.quickSearch(key)
	if r == nil { // impossible run to this
		t_log.Log(t_log.PANIC, "error point in bohm op\n")
	}
	(*wv)[key].Write()
	return 1, true
}


func (t *TXN) Read(key string) (int, bool) {

	r := t.quickGetOrInsert(key, NewRecord())
	step, ok := r.Read(t.txn_id)

	return step, ok
}


func (t *TXN) Install() (int, bool) {
	step := 1
	for key, count := range *t.c_write_set {
		r := t.quickGetOrInsert(key, NewRecord())
		s, v, _ := r.Install(t.txn_id, count)
		step = step + s
		(*t.write_version)[key] = v
	}
	return step, true
}


func (t *TXN) Commit() (int, bool) {
	w_vs := t.write_version
	step := 0
	for _, v := range *w_vs {
		step ++
		v.Commit()
	}
	return step, true
}