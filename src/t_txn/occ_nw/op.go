package occ_nw

import (
	"t_index"
	"t_txn/occ_nw/rd"
	"t_txn"
	"sync"
)

type OCC struct {
	index *(t_index.Mmap)
	max_commit_id int // no assign id (assign count)
	m *sync.Mutex
}

func (occ *OCC) GetCommitID() int {
	occ.m.Lock()
	defer occ.m.Unlock()
	commit_id := occ.max_commit_id
	occ.max_commit_id = commit_id + 1
	return commit_id

}


func (occ *OCC) PreparationCost(thread_cnt int, opss [](*t_txn.OPS)) int {
	return 0
}

// not snapshot read so do not need reset
func (occ *OCC) Reset() {

}

func New(mmap_c int) *OCC {
	index := t_index.NewMmap(mmap_c)
	return &OCC{index, 0, &(sync.Mutex{})}
}

func quickGetOrInsert(index *(t_index.Mmap), key string) *rd.Record {
	r := index.Search(key)
	if r == nil {
		r = index.GetOrInsert(key, rd.NewRecord())
	}
	return r.(*rd.Record)
}


type TXN struct {
	txn_id int
	commit_id int

	base *OCC
	write_set *map[*rd.Record]bool // for NO_WAIT release
}


/*
occ need to know nothing
*/
func (r *OCC) Prios(ops *(t_txn.OPS)) {
	return 
}


/*
occ need to know read write set
*/
func (occ *OCC) NewTXN(txn_id int, ops *(t_txn.OPS)) t_txn.TxnPtr {
	occ.Prios(ops)
	
	return &TXN{txn_id, occ.GetCommitID(), occ, &(map[*rd.Record]bool{})}
}



func (t *TXN) Write(key string) int {
	index := t.base.index
	r := quickGetOrInsert(index, key)
	if r.Write(t.commit_id) == true {
		(*t.write_set)[r] = true
		return t_txn.NEXT
	} else {
		return t_txn.RERUN
	}
}


func (t *TXN) Read(key string) int {
	index := t.base.index
	r := quickGetOrInsert(index, key)
	if r.Read(t.commit_id) == true {
		return t_txn.NEXT
	} else {
		return t_txn.RE2LAST
	}
}


/*
Reset must ok
*/
func (t *TXN) Reset() int {
	for r, v := range(*t.write_set) {
		if v == true {
			r.DoneWrite(t.commit_id)
			(*t.write_set)[r] = false
		}
	}
	t.commit_id = t.base.GetCommitID()
	return t_txn.NEXT
}

func (t *TXN) Commit() int {
	for r, v := range(*t.write_set) {
		if v == true {
			r.Commit(t.commit_id)
			r.DoneWrite(t.commit_id)
			v = false
		}
	}
	return t_txn.NEXT
}

func (t *TXN) Init() int {
	return t_txn.NEXT
}