package gria

import (
	"t_index"
	"t_log"
	"t_txn"
	"container/list"
	"fmt"
	"sync"
	"t_util"
)

type Gria struct {
	// batch_size configure by user
	index *(t_index.Mmap)
	max_commit_id int
	rwlock *sync.RWMutex
}

func (g *Gria) GetCommitID() int {
	g.rwlock.Lock()
	defer g.rwlock.Unlock()
	commit_id := g.max_commit_id
	g.max_commit_id = commit_id + 1
	return commit_id

}


func (g *Gria) Reset() {
	g.index = g.index.ReNew()
	
}

/*
return the max thread op not the core op
*/
func (g *Gria) PreparationCost(thread_cnt int, opss []t_txn.AccessPtr) int {

	return 0
}



func New(mmap_c int) *Gria {
	index := t_index.NewMmap(mmap_c)
	return &(Gria{index, 0, &(sync.RWMutex{})})
}


const (
	PENDING = 0
	COMMIT = 1
	ABORT = 2
)

type TXN struct {
	txn_id int
	group_id int
	
	commit_id int
	read_elements *(map[*(list.Element)]bool) // not include read own write
	// read_own_elements *(map[*(list.Element)]bool) // only read own write
	write_elements *(map[*(list.Element)]bool) // for reordering

	anti_dependence	*(map[*TXN]bool) // change this is thread safe !
	dependence_abort bool

	fb_state int

	base *Gria
}


func (t *TXN) PrintReadVersins() string {
	var res string
	for ele := range(*(t.read_elements)) {
		v := ele.Value.(*Version)
		res = res + fmt.Sprintf("[%d|%d] ", v.owner_txn.commit_id, v.owner_txn.group_id)
	}
	return res
}


/*Aria need no prios*/
func (g *Gria) Prios(ops t_txn.AccessPtr) {
	return
}

/*
when init set the group_id
*/
func (g *Gria) NewTXN(txn_id int, ops t_txn.AccessPtr) t_txn.TxnPtr {
	g.Prios(ops)
	commit_id := g.GetCommitID()
	return &TXN{txn_id, -1, commit_id, &(map[*(list.Element)]bool{}), &(map[*(list.Element)]bool{}), &(map[*TXN]bool{}), false, PENDING, g}
}

func quickGetOrInsert(index *(t_index.Mmap), key string) *Record { 
	r := index.Search(key)
	if r == nil {
		r = index.GetOrInsert(key, NewRecord())
	}
	return r.(*Record)
}


func (t *TXN) Write(key string) int {
	index := t.base.index
	r := quickGetOrInsert(index, key)
	if r == nil { // impossible run to this
		t_log.Log(t_log.PANIC, "error point in gria op\n")
	}
	ele := r.Write(t)
	(*(t.write_elements))[ele] = true 

	return t_txn.NEXT
}


func (t *TXN) Read(key string) int {
	// t_log.Log(t_log.DEBUG, "commit: %d read %v\n", t.commit_id, key)
	index := t.base.index

	r := quickGetOrInsert(index, key)
	if r != nil {
		ele := r.Read(t)
		v := ele.Value.(*Version)
		if v.owner_txn.txn_id != t.txn_id {
			(*(t.read_elements))[ele] = true 
		}

		// change the anti-dependence of the reading version owner transaction for cascading abort
		dep_txn := ele.Value.(*Version).owner_txn
		if dep_txn.group_id != -1 { // latest snapshot will not abort and do not count in dependence link
			(*(dep_txn.anti_dependence))[t] = true
		}

	}
	return t_txn.NEXT
}

/*
used when begin the txn operations
the parameter is gria exclusive
*/
func (t *TXN) Init(group_id int) int {
	t.group_id = group_id
	return t_txn.NEXT
}

// with new commit_id 
func (t *TXN) Reset() int {
	commit_id := t.base.GetCommitID()
	t.commit_id = commit_id
	t.read_elements = &(map[*(list.Element)]bool{})
	t.write_elements = &(map[*(list.Element)]bool{})
	t.anti_dependence = &(map[*TXN]bool{})
	t.dependence_abort = false
	return t_txn.NEXT
}

func (g *Gria) PrintRecordList(key string) string {
	r := g.index.Search(key).(*Record)
	return r.PrintList()
}


/* read the snapshot may cause conflict but must enable to reorder to pass */
func (t *TXN) Commit() int {
	if t.dependence_abort == true {
		t.Cascading_abort()
		return t_txn.NEXTBATCH_CA
	}
	ro := false
	// check the RAW dependence between group
	r_set := t.read_elements
	// t_log.Log(t_log.DEBUG, "Commit Begin, read count:%d, read Commit:%d, my [%d|%d]\n", len((*r_set)), t.commit_id, t.group_id)
	for ele, _ := range (*r_set) { // loop read version

		next_ele := ele.Next()
		if next_ele != nil {
			next_version := next_ele.Value.(*Version)

			if next_version.owner_txn.group_id != t.group_id { // actually the next the read version can detect conflict or not
					// t_log.Log(t_log.DEBUG, "read_versin: [%d|%d], my [%d|%d], behind_version: [%d|%d]\n", ele.Value.(*Version).owner_txn.commit_id, ele.Value.(*Version).owner_txn.group_id, t.commit_id, t.group_id, behind_version.owner_txn.commit_id, behind_version.owner_txn.group_id)
				if next_version.owner_txn.commit_id < t.commit_id { // this must be the first next version!
						// reorder begin
						if t_util.Pconf.Reordering == true && t.GetCommitRange() {
							ro = true
							break
						}
						// reorder end
						t.Cascading_abort()
						return t_txn.NEXTBATCH_RAW
				}
			}
		}
	}
	// t_log.Log(t_log.DEBUG, "Commit End\n")
	t.fb_state = COMMIT
	if ro == true {
		return t_txn.RO_NEXT
	}
	return t_txn.NEXT
}

// change the anti-dependence transaction's dependence abort
func (t *TXN) Cascading_abort() {
	for anti_dep_txn, _ := range(*(t.anti_dependence)) {
		anti_dep_txn.dependence_abort = true
	}
}


func (t *TXN) GetSmallestCommitID() int {

	// loop the write set
	w_set := t.write_elements
	smallest_cid := -1
	for ele, _ := range (*w_set) {

		for prev_ele := ele.Prev(); prev_ele != nil; prev_ele = prev_ele.Prev() {
			prev_version := prev_ele.Value.(*Version)
			if prev_version.max_read_txn != nil { // this write validate end (the snapshot version's max_read_txn is nil)
				if smallest_cid < prev_version.max_read_txn.commit_id {
					smallest_cid = prev_version.max_read_txn.commit_id
					t_log.Log(t_log.DEBUG, "txn_id:%d\tsmallest_cid:%d\n", t.commit_id, smallest_cid)
				}
				break
			}
		}
	}
	// t_log.Log(t_log.INFO, "txn_id:%d, smallest_cid:%d\n", t.commit_id, smallest_cid)
	return smallest_cid
}


func (t *TXN) GetCommitRange() bool {
	INF := 100000000
	max_rv := -1
	min_rnv := INF
	r_set := t.read_elements
	w_set := t.write_elements

	for ele, _ := range (*r_set) {
		n_ele := ele.Next()
		v := ele.Value.(*Version)
		if max_rv < v.owner_txn.txn_id {
			max_rv = v.owner_txn.txn_id
		}
		if n_ele != nil {
			nv := n_ele.Value.(*Version)
			if nv.owner_txn.txn_id < min_rnv {
				min_rnv = nv.owner_txn.txn_id
			}
		}
	}

	max_wv := -1
	for ele, _ := range (*w_set) {
		for p_ele := ele.Prev(); p_ele != nil; p_ele = p_ele.Prev() {
			pv := p_ele.Value.(*Version)
			if max_wv < pv.owner_txn.txn_id {
				max_wv = pv.owner_txn.txn_id
			}
			if pv.max_read_txn != nil && max_wv < pv.max_read_txn.txn_id {
				max_wv = pv.max_read_txn.txn_id
				break
			}
		}
	}

	if max_rv < min_rnv && max_wv < min_rnv {
		return true
	} else {
		return false
	}

}


func (t *TXN) FallBack() int {
	var cur_fb_read *list.Element
	for cur_fb_read, _ = range *(t.read_elements) {
		break
	}
	// t_log.Log(t_log.INFO, "%v\n", *(t.read_elements))
	if cur_fb_read != nil {
		for ele := cur_fb_read.Next(); ele != nil; ele = ele.Next() {
			v := ele.Value.(*Version)
			if v.owner_txn.txn_id < t.txn_id {
				if v.owner_txn.fb_state == PENDING {
					// t_log.Log(t_log.INFO, "%v is pending\n", v.owner_txn.txn_id)
					return t_txn.FB_AGAIN_BLOCK
				}
				if v.owner_txn.fb_state == COMMIT {
					delete(*(t.read_elements), cur_fb_read)
					t.fb_state = ABORT
					return t_txn.FB_ABORT
				}
			} else {
				v = cur_fb_read.Value.(*Version)
				if v.owner_txn.fb_state == PENDING {
					// t_log.Log(t_log.INFO, "%v is pending\n", v.owner_txn.txn_id)
					return t_txn.FB_AGAIN_BLOCK
				}
				if v.owner_txn.fb_state == ABORT {
					delete(*(t.read_elements), cur_fb_read)
					t.fb_state = ABORT
					return t_txn.FB_ABORT
				} else {
					break
				}
			}
		}
		delete(*(t.read_elements), cur_fb_read)
		return t_txn.FB_AGAIN
	} else {
		t.fb_state = COMMIT
		return t_txn.FB_COMMIT
	}
}