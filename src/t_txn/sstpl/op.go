package sstpl

import (
	"t_index"
	"t_txn/sstpl/rd"
	"t_txn"
	"sort"
)
/*
use for test coro and the sample of the txn model
*/

type SSTPL struct {
	index *(t_index.Mmap)
}

// not snapshot read so do not need reset
func (sstpl *SSTPL) Reset() {

}



func (sstpl *SSTPL) PreparationCost(thread_cnt int, opss [](*t_txn.OPS)) int {
	return 0
}


func New(mmap_c int) *SSTPL {
	index := t_index.NewMmap(mmap_c)
	return &SSTPL{index}
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
	base *SSTPL
	// the read write set for add the sequence lock
	lock_list *([](*(t_txn.OP)))
	cur_lock int // cur is not lock
}


/*
sstpl need to know read write set
*/
func (r *SSTPL) Prios(ops *(t_txn.OPS)) (write_set, read_set *map[string]bool) {
	return ops.ReadWriteSet(true), ops.ReadWriteSet(false)
}


/*
sstpl need to know read write set
*/
func (r *SSTPL) NewTXN(txn_id int, ops *(t_txn.OPS)) t_txn.TxnPtr {
	write_set, read_set := r.Prios(ops)
	ll := make([](*(t_txn.OP)), len(*write_set) + len(*read_set))
	i := 0
	for k, _ := range (*write_set) {
		ll[i] = &(t_txn.OP{k, true})
		i ++
	}
	for k, _ := range (*read_set) {
		ll[i] = &(t_txn.OP{k, false})
		i ++
	}

	sort.Slice(ll, func(i, j int) bool {
		return ll[i].Less(ll[j])
	})

	return &TXN{txn_id, r, &ll, 0}
}

func (t *TXN) lockKeys(op *(t_txn.OP)) bool {

	index := t.base.index
	for i := t.cur_lock; i < len(*(t.lock_list)); i++ {
		l_op := (*(t.lock_list))[i]
		if l_op.LessOrEqual(op) {
			// get or new the record
			r := quickGetOrInsert(index, l_op.Key)
			// add lock
			if l_op.Is_write == true {
				if r.Write(t.txn_id) == false {
					return false
				}
			} else {
				if  r.Read(t.txn_id) == false {
					return false
				}
			}
			// update lock index
			t.cur_lock = i + 1
		}
	}
	return true
}


func (t *TXN) Write(key string) int {
	op := t_txn.OP{key, true}
	if t.lockKeys(&op) == true {
		return t_txn.NEXT
	} else {
		return t_txn.AGAIN
	}
}


func (t *TXN) Read(key string) int {
	op := t_txn.OP{key, false}
	if t.lockKeys(&op) == true {
		return t_txn.NEXT
	} else {
		return t_txn.AGAIN
	}
}

func (t *TXN) Reset() int {
	index := t.base.index
	for i := t.cur_lock - 1; i >= 0; i -- {
		key := (*t.lock_list)[i].Key
		is_w := (*t.lock_list)[i].Is_write
		
		r := quickGetOrInsert(index, key)

		if is_w {
			r.DoneWrite(t.txn_id)
		} else {
			r.DoneRead(t.txn_id)
		}
	}
	return t_txn.NEXT
}


func (t *TXN) Commit() int {
	return t.Reset()
}


func (t *TXN) Init() int {
	return t_txn.NEXT
}

