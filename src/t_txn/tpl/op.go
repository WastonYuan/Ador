package tpl

import (
	"t_index"
	"t_txn/tpl/rd"
	"t_txn"
	"sort"
)
/*
use for test coro and the sample of the txn model
*/

type TPL struct {
	index *(t_index.Mmap)
}


func New(mmap_c int) *TPL {
	index := t_index.NewMmap(mmap_c)
	return &TPL{index}
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
	base *TPL
	// the read write set for add the sequence lock
	lock_list *([](*(t_txn.OP)))
	cur_lock int // cur is not lock

	write_map *map[string]int
	read_map *map[string]int
}


/*
sstpl need to know read write map
*/
func (r *TPL) Prios(ops *(t_txn.OPS)) (write_map, read_map *map[string]int) {
	return ops.ReadWriteMap(true), ops.ReadWriteMap(false)
}


func (r *TPL) PreparationCost(thread_cnt int, opss [](*t_txn.OPS)) int {
	return 0
}

func (r *TPL) Reset() {
	
}


/*
sstpl need to know read write set
*/
func (r *TPL) NewTXN(txn_id int, ops *(t_txn.OPS)) t_txn.TxnPtr {
	write_map, read_map := r.Prios(ops)
	ll := make([](*(t_txn.OP)), len(*write_map) + len(*read_map))
	i := 0
	for k, _ := range (*write_map) {
		ll[i] = &(t_txn.OP{k, true})
		i ++
	}
	for k, _ := range (*read_map) {
		ll[i] = &(t_txn.OP{k, false})
		i ++
	}

	sort.Slice(ll, func(i, j int) bool {
		return ll[i].Less(ll[j])
	})

	return &TXN{txn_id, r, &ll, 0, write_map, read_map}
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
	index := t.base.index
	op := t_txn.OP{key, true}
	if t.lockKeys(&op) == true {
		// check can be release or not
		w_c := (*(t.write_map))[key] - 1
		(*(t.write_map))[key] = w_c
		if w_c == 0 {
			r := quickGetOrInsert(index, key)
			r.DoneWrite(t.txn_id)
		}
		return t_txn.NEXT
	} else {
		return t_txn.AGAIN
	}
}


func (t *TXN) Read(key string) int {
	index := t.base.index
	op := t_txn.OP{key, false}
	if t.lockKeys(&op) == true {
		// check can be release or not
		r_c := (*(t.read_map))[key] - 1
		(*(t.read_map))[key] = r_c
		if r_c == 0 {
			r := quickGetOrInsert(index, key)
			r.DoneRead(t.txn_id)
		}
		return t_txn.NEXT
	} else {
		return t_txn.AGAIN
	}
}

func (t *TXN) Reset() int {
	index := t.base.index
	for key, w_c:= range *(t.write_map) {
		if w_c > 0 {
			r := quickGetOrInsert(index, key)
			r.DoneWrite(t.txn_id)
			(*(t.write_map))[key] = 0
		}
	}
	for key, r_c:= range *(t.read_map) {
		if r_c > 0 {
			r := quickGetOrInsert(index, key)
			r.DoneRead(t.txn_id)
			(*(t.read_map))[key] = 0
		}
	}
	return t_txn.NEXT
}

/*
Commit is the same as Reset
*/
func (t *TXN) Commit() int {
	return t.Reset()
}

func (t *TXN) Init() int {
	return t_txn.NEXT
}