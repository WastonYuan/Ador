package mvcc


import (
	"t_index"
	"t_txn/mvcc/rd"
	"t_txn"
	"sync"
	"t_log"
)


type MVCC struct {
	index *(t_index.Mmap)
	max_commit_id int // no assign id (assign count)
	m *sync.Mutex
}


func (mvcc *MVCC) PreparationCost(thread_cnt int, opss [](*t_txn.OPS)) int {
	return 0
}


// not snapshot read so do not need reset
func (mvcc *MVCC) Reset() {

}

func (mvcc *MVCC) GetCommitID() int {
	mvcc.m.Lock()
	defer mvcc.m.Unlock()
	commit_id := mvcc.max_commit_id
	mvcc.max_commit_id ++
	return commit_id

}

func New(mmap_c int) *MVCC {
	index := t_index.NewMmap(mmap_c)
	return &MVCC{index, 0, &(sync.Mutex{})}
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
	base *MVCC

	write_version *map[*rd.Version]bool
	is_init bool

}


/*
mvcc need no prios
*/
func (mvcc *MVCC) Prios(ops *(t_txn.OPS))  {
	return 
}


/*
sstpl need to know read write set
*/
func (mvcc *MVCC) NewTXN(txn_id int, ops *(t_txn.OPS)) t_txn.TxnPtr {
	mvcc.Prios(ops)

	return &TXN{txn_id, 0, mvcc, &(map[*rd.Version]bool{}), false}
}




func (t *TXN) Write(key string) int {
	index := t.base.index
	wv := t.write_version
	r := quickGetOrInsert(index, key)

	v := r.Write(t.commit_id)
	if v == nil {
		return t_txn.RE2LAST
	} else {
		(*wv)[v] = true // save the writing version for revert
		return t_txn.NEXT
	}
}


func (t *TXN) Read(key string) int {
	index := t.base.index

	r := quickGetOrInsert(index, key)
	if r.Read(t.commit_id) {
		return t_txn.NEXT
	} else {
		return t_txn.AGAIN
	}
}

/*
must success
*/
func (t *TXN) Reset() int {
	
	wv := t.write_version
	for version, v := range (*wv) {
		if v == true {
			(*wv)[version] = false
			version.Abort()
		}
	}
	t.is_init = false
	return t_txn.NEXT
}


/*
must success
*/
func (t *TXN) Commit() int {
	
	wv := t.write_version
	for version, v := range (*wv) {
		if v == true {
			(*wv)[version] = false
			version.Commit()
		}
	}
	return t_txn.NEXT
}


func (t *TXN) Init() int {
	if t.is_init == false {
		t.is_init = true
		t.commit_id = t.base.GetCommitID()
		t_log.Log(t_log.INFO, "get commitid:%v\n", t.commit_id)
	}
	return t_txn.NEXT
}


