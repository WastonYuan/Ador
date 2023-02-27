package pwv_nw

import (
	"t_index"
	"t_txn/pwv_nw/rd"
	"t_log"
	"sync"
	"t_txn"
)


type DEBUG struct {
	keys *sync.Map
}

type PWV struct {
	// batch_size configure by user
	index *(t_index.Mmap)
	Read_conflict int	
}

func (pwv *PWV) Reset() {
}

/*
return the max thread op not the core op
*/
func (pwv *PWV) PreparationCost(thread_cnt int, opss [](*t_txn.OPS)) int {
	sum_c := 0
	for i := 0; i < len(opss); i ++ {
		sum_c = sum_c + opss[i].Len()
	}
	return sum_c / thread_cnt
}

func New(mmap_c int) *PWV {
	index := t_index.NewMmap(mmap_c)
	return &(PWV{index, 0})
}

type TXN struct {
	txn_id int
	write_map *(map[string]int) // pwv should know the write set in start
	write_version *(map[*(rd.Version)]bool) // true means being writed which will clean when revert
	o_read_version *(map[*(rd.Version)]bool) // for pwv read the modified version and waiting for it staged and will clean when revert
	base *PWV
}

/*pwv need to know the write map*/
func (pwv *PWV) Prios(ops *(t_txn.OPS)) *(map[string]int) {
	return ops.ReadWriteMap(true)
}

func (pwv *PWV) NewTXN(txn_id int, ops *(t_txn.OPS)) t_txn.TxnPtr {
	write_map := pwv.Prios(ops)
	txn := TXN{txn_id, write_map, &(map[*(rd.Version)]bool{}), &(map[*(rd.Version)]bool{}), pwv}
	txn.InstallKeys()
	return &txn
}

func (pwv *PWV) GetWriteConflict() int {
	return pwv.Read_conflict
}


func quickGetOrInsert(index *(t_index.Mmap), key string) *rd.Record {
	r := index.Search(key)
	if r == nil {
		r = index.GetOrInsert(key, rd.NewRecord())
	}
	return r.(*rd.Record)
}

/*
first phase write (for use instead of InstallKeys)
*/
func (t *TXN) Install(key string)  {
	index := t.base.index
	// find or insert the key to index
	r := quickGetOrInsert(index, key)
	// r.install
	r.Install(t.txn_id, rd.PENDING)
}

/*
Install all keys in the first phase
*/
func (t *TXN) InstallKeys() {
	wm := t.write_map
	for key, count := range (*wm) {
		for i := 0; i < count; i ++ {
			t.Install(key)
		}
	}
}



func (t *TXN) Write(key string) int {
	index := t.base.index
	wv := t.write_version
	r := index.Search(key)
	if r == nil { // impossible run to this
		t_log.Log(t_log.PANIC, "error point pwv op\n")
	}
	v := r.(*(rd.Record)).Write(t.txn_id)
	if v == nil {
		return t_txn.RE2FIRST
	} else {
		(*wv)[v] = true // save the writing version for revert
		return t_txn.NEXT
	}
}


func (t *TXN) Read(key string) int {
	index := t.base.index
	orv := t.o_read_version

	r := quickGetOrInsert(index, key)
	v, is_n := r.Read(t.txn_id)
	if is_n {
		v = r.Install(-1, rd.COMMITED)
	}
	if v == nil {
		// t_log.Log(t_log.DEBUG, "txn %v read key %v failed in vl %v\n", t.txn_id, key, r.VersionListString())
		return t_txn.RE2FIRST
	} else {
		if v.GetTXN() != t.txn_id {
			(*orv)[v] = true
		}
		return t_txn.NEXT
	}
}

/*
logically it will always return true
*/
func (t *TXN) Reset() int {
	return t_txn.NEXT
}


/*
Toro also can improve in this place
*/
func (t *TXN) CheckOtherTXNStaged() bool {
	orv := t.o_read_version
	for version, _ := range (*orv) {
		if version.GetStats() < rd.STAGED {
			// t_log.Log(t_log.DEBUG, "txn %v hang: %v\n", t.txn_id, version.GetString())
			return false
		}
	}
	return true
}


func (t *TXN) Commit() int {
	if t.CheckOtherTXNStaged() == false {
		return t_txn.AGAIN
	} else {
		wv := t.write_version
		for version, _ := range (*wv) {
			if version.GetStats() == rd.MODIFIED {
				version.UpdateStats(rd.STAGED)
			} else {
				t_log.Log(t_log.PANIC, "Staged failed Version: %v", version.GetString())
			}
		}
	}
	return t_txn.NEXT
}

func (t *TXN) Init() int {
	return t_txn.NEXT
}