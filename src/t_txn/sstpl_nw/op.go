package sstpl_nw

import (
	"t_index"
	"t_txn/sstpl_nw/rd"
	"t_txn"
)
/*
use for test coro and the sample of the txn model
*/

type NW_SSTPL struct {
	index *(t_index.Mmap)
}


func New(mmap_c int) *NW_SSTPL {
	index := t_index.NewMmap(mmap_c)
	return &NW_SSTPL{index}
}

func quickGetOrInsert(index *(t_index.Mmap), key string) *rd.Record {
	r := index.Search(key)
	if r == nil {
		r = index.GetOrInsert(key, rd.NewRecord())
	}
	return r.(*rd.Record)
}

func (sstpl *NW_SSTPL) PreparationCost(thread_cnt int, opss [](*t_txn.OPS)) int {
	return 0
}

// not snapshot read so do not need reset
func (sstpl *NW_SSTPL) Reset() {

}


type TXN struct {
	txn_id int
	base *NW_SSTPL
	// the read write set for finsished and reset
	write_set *map[*(rd.Record)]bool
	read_set *map[*(rd.Record)]bool
}


/*no wait ss2pl need to know nothing*/
func (r *NW_SSTPL) Prios(ops *(t_txn.OPS)) {
	return
}

/*
sstpl need to know nothing
*/
func (r *NW_SSTPL) NewTXN(txn_id int, ops *(t_txn.OPS)) t_txn.TxnPtr {
	r.Prios(ops)
	return &TXN{txn_id, r, &(map[*(rd.Record)]bool{}), &(map[*(rd.Record)]bool{})}
}

func (t *TXN) Write(key string) int {
	index := t.base.index

	r := quickGetOrInsert(index, key)

	if r.Write(t.txn_id) == true {
		(*t.write_set)[r] = true
		return t_txn.NEXT
	} else {
		return t_txn.RERUN
	}

}


func (t *TXN) Read(key string) int {
	index := t.base.index

	r := quickGetOrInsert(index, key)

	if r.Read(t.txn_id) == true {
		(*t.read_set)[r] = true
		return t_txn.NEXT
	} else {
		return t_txn.RERUN
	}
}

func (t *TXN) Reset() int {
	for r, _ := range(*(t.write_set)) {
		r.DoneWrite(t.txn_id)
	}
	t.write_set = &(map[*(rd.Record)]bool{})
	for r, _ := range(*(t.read_set)) {
		r.DoneRead(t.txn_id)
	}	
	t.read_set = &(map[*(rd.Record)]bool{})
	return t_txn.NEXT
}


func (t *TXN) Commit() int {
	return t.Reset()
}

func (t *TXN) Init() int {
	return t_txn.NEXT
}