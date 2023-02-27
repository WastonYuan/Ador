package ador4docc

import (
	// "t_log"
	// "t_txn"
	// "container/list"
	// "sync"
	// "t_util"
)



type WriteReq struct {
	Key string
	// data
}

func MakeWriteReq(key string) WriteReq {
	return WriteReq{key}
}

/*
only save the key not record
*/
type ReadReq struct {
	// request
	Key string // if multi-read the key is the left range.
	Len int // if single read the len is 0
} 


func (rq *ReadReq) IsSingleRead() bool {
	return rq.Len == 0
}


func MakeMultiReadReq(key string, s_len int) ReadReq { 
	return ReadReq{key, s_len}
}

func MakeSingleReadReq(key string) ReadReq {
	return ReadReq{key, 0}
}

type TXN struct {
	txn_id int
	coro *Coroutine
	base *Ody

	has_rebase bool

	read_req_set *(map[ReadReq]bool) // for same read only store onece (by read_seq once!)
	read_req_seq *([](*ReadReq)) // store each read record in op sequence (write does not cause conflict since only one txn commit)
	// just store once for the same record (by read_record) but multi-read will store multi times
	// we need to prove that the same read only verfy onece.
	key2rts *(map[string](int)) // key and read version for conflict validation, each record just store onece
	// since read own write must not cause conflict
	m_req2keys *(map[ReadReq](*([]string))) // read to keys for phantom read validation.
	// first validate phantom read then validate the conflict read for the multi-read
	write_records *(map[*Record](bool)) // writing record for abort (clear the thread_wts) and commit
	write_req_seq *([](*WriteReq)) // write req list for rebase
	read_req2write_to *(map[ReadReq]int) // for rebase

	key2record *(map[string](*Record)) // for validate conflict by seq(key to record)
	// r_key_cache *(map[*Record](string))
	
}


func (o *Ody) NewTXN(txn_id int, coro *Coroutine) *TXN {

	read_req_set := map[ReadReq]bool{}
	read_req_seq := [](*ReadReq){}

	key2rts := map[string](int){} // must exist while read a key onece, if no record then rts = -1

	m_req2keys := map[ReadReq](*([]string)){}

	write_records := map[*Record](bool){}

	write_req_seq := [](*WriteReq){}
	read_req2write_to := map[ReadReq]int{}

	key2record := map[string](*Record){} // may exist while read a key onece (not that record)


	return &TXN{txn_id, coro, o, false,
		&read_req_set, &read_req_seq, &key2rts, &m_req2keys, 
		&write_records, &write_req_seq, &read_req2write_to, &key2record}
} 


func (t *TXN) quickGetOrInsert(key string, n_r *Record) *Record {
	key_r := t.key2record
	base := t.base
	r, ok := (*key_r)[key]
	if ok == false {
		r = base.quickGetOrInsert(key, n_r)
		(*key_r)[key] = r
	}
	return r
}

func (t *TXN) quickSearch(key string) *Record {
	key_r := t.key2record
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
if previous exist such scan, then re-read the key before.
*/
func (t *TXN) MultiRead(key string, s_len int) int {
	base := t.base

	// dealing with the request
	req := MakeMultiReadReq(key, s_len)
	_, is_exist := (*t.read_req_set)[req]
	// only handle the req never done, if do it before the validation can be pass since
	// 1) phantom problem, if former happened then we do not need to handle the behind (even if it may not happeneed), if former pass then behind is also pass
	// 2) normal conflict (no phantom problem), second read record must be the same as former read, so a same read we just validate once (since first ok second must ok first fail second do not need validation). 
	if is_exist == false {
		if t.has_rebase == false {
			//for rebase
			(*t.read_req2write_to)[req] = len((*t.write_req_seq))
			// for conflict validation
			(*t.read_req_set)[req] = true
			(*t.read_req_seq) = append(*t.read_req_seq, &req)
		}

		keys := []string{}

		fetch_cnt := 0
		for key, r, ele := base.RangeSearch(key); ele != nil && fetch_cnt != s_len; key, r, ele =  base.RangNext(ele) {
			rts := r.Read(t)
			if rts != -1 {
				keys = append(keys, key)
				_, is_exist := (*t.key2rts)[key] // validate also save onece.
				if is_exist == false {
					if t.has_rebase == false {
						(*t.key2rts)[key] = rts // for conflict validation
						(*t.key2record)[key] = r
					}
					fetch_cnt ++
				}
			}
		}

		// for phantom validation
		if t.has_rebase == false {
			(*t.m_req2keys)[req] = &keys
		}
	}
	return len(*(*t.m_req2keys)[req]) 
}

/*
read must success
*/
func (t *TXN) SingleRead(key string) {
	if t.has_rebase == true {
		return
	}
	// dealing with the request first
	req := MakeSingleReadReq(key)
	_, is_exist := (*t.read_req_set)[req]
	// if exist then it will not cause conflict read pass, if record not exist then the read must be the 
	if is_exist == false {
		//for rebase
		(*t.read_req2write_to)[req] = len((*t.write_req_seq))
		// no matter the record exist or not we need to save the req
		(*t.read_req_set)[req] = true
		(*t.read_req_seq) = append(*t.read_req_seq, &req)
		r := t.quickSearch(key) 

		// save rts for normal conflict validation
		if r != nil {
			rts := r.Read(t)
			_, is_exist := (*t.key2rts)[key] // validate also save onece.
			if is_exist == false {
				(*t.key2rts)[key] = rts // for conflict validation
				(*t.key2record)[key] = r
			}
		} else { // multi-read will not be nil
			(*t.key2rts)[key] = -1
		}
	} 
}

/*
write must success
*/
func (t *TXN) Write(key string) {

	r := t.quickGetOrInsert(key, NewRecord())
	r.Write(t)
	(*t.write_records)[r] = true
	if t.has_rebase == false {
		req := MakeWriteReq(key)
		(*t.write_req_seq) = append((*t.write_req_seq), &req)
	}
}

/*
reset writing record in rebase
*/
func (t *TXN) Rebase(key string, s_len int) int {
	
	// reset the write_record (used for commit)
	t.write_records = &(map[*Record](bool){})
	// reset end

	var req ReadReq
	if s_len == 0 {
		req = MakeSingleReadReq(key)
	} else {
		req = MakeMultiReadReq(key, s_len)
	}
	write_to := (*t.read_req2write_to)[req]
	for i := 0; i < write_to; i++ {
		key := (*t.write_req_seq)[i].Key
		r := t.quickGetOrInsert(key, NewRecord())
		r.Write(t)
		(*t.write_records)[r] = true
	}

	t.has_rebase = true // next exec must no conflict and next commit must success.
	return write_to
 
}


/*
abort must success
the txn (coroutine)
*/
func (t *TXN) Abort() {
	// first clear the thread_wts of the writing record.
	
	// second clear all class variable
	read_req_set := map[ReadReq]bool{}
	read_req_seq := [](*ReadReq){}
	key2rts := map[string](int){} // must exist while read a key onece, if no record then rts = -1
	m_req2keys := map[ReadReq](*([]string)){}
	write_records := map[*Record](bool){}
	write_req_seq := [](*WriteReq){}
	read_req2write_to := map[ReadReq]int{}
	key2record := map[string](*Record){} // may exist while read a key onece (not that record)

	t.read_req_set = &read_req_set
	t.read_req_seq = &read_req_seq
	t.key2rts = &key2rts
	t.m_req2keys = &m_req2keys
	t.write_records = &write_records
	t.write_req_seq = &write_req_seq
	t.read_req2write_to = &read_req2write_to
	t.key2record = &key2record

}


/*
key, s_len, is_phantom, ok
*/
func (t *TXN) Validate() (string, int, bool, bool) {
	base := t.base
	for _, read_req := range(*t.read_req_seq) {
		// t_log.Log(t_log.DEBUG, "single read validate:%v\n", read_req)
		// first check the single read.
		if read_req.IsSingleRead() {
			key := read_req.Key
			if (*t.key2rts)[key] == -1 { // no this record when read
				// if rts == -1, then check the record is not exist or the record is not available
				r := base.Search(key)
				if r == nil {
					continue
				} else {
					if r.Validate(t, -1) == false {
						return key, 0, false, false
					} else {
						continue
					}
				}
			} else {
				// check normal conflict
				rts := (*t.key2rts)[key]
				rd := (*t.key2record)[key]
				res := rd.Validate(t, rts)
				if res == false {
					return key, 0, false, false
				} else {
					continue
				}
			}
		// then check the multi read
		} else {
			// t_log.Log(t_log.DEBUG, "muti read validate:%v\n", read_req)
			scan_left := read_req.Key
			scan_len := read_req.Len
			keys := (*t.m_req2keys)[*read_req]
			// t_log.Log(t_log.DEBUG, "muti read keys::%v\n", keys)
			// first check phantom
			for i, key := range(*keys) {
				/*
				for each key first check the phantom problem
				*/
				// t_log.Log(t_log.DEBUG, "muti read check phantom conflict: %v\n", keys)
				if i == 0 {
					// GetPrevKey must return a available record key, i.e. a key with a wts != -1. 
					for prev_key, prev_r, ok := base.GetPrevKey(key); ok && scan_left <= prev_key; prev_key, prev_r, ok = base.GetPrevKey(prev_key) {
						// t_log.Log(t_log.PANIC, "get prev key:%v, muti read check phantom continue\n", prev_key)
						if prev_r.IsEmpty() { // ignore the empty record.
							continue
						}
						if prev_r.IsReadable(t) { // there are a record can be read but not scan. phantom read conflict.
							return scan_left, scan_len, true, false
						}
					}
				} else {
					for prev_key, prev_r, ok := base.GetPrevKey(key); ok && (*keys)[i-1] < prev_key; prev_key, prev_r, ok = base.GetPrevKey(prev_key) {
						if prev_r.IsEmpty() { // ignore the empty record.
							continue
						}
						if prev_r.IsReadable(t) { // there are a record can be read but not scan. phantom read conflict.
							return scan_left, scan_len, true, false
						}
					}
				}
				// t_log.Log(t_log.DEBUG, "muti read check conflict: %v\n", keys)
				/*
				then check the normal conflict of this key (by multi read)
				*/
				rd := (*t.key2record)[key] 
				rts := (*t.key2rts)[key]
				res := rd.Validate(t, rts)
				if res == false {
					return scan_left, scan_len, false, false
				} else {
					continue
				}
			}
		}
	}
	return "", 0, false, true
}




// return the key and scan_len (not the scan records) (if len == 1 then is single read)
// if commit success return "", 0, true
// only reach the max_commit txn can run the Commit() so if commit fail then the Txn must be abort 
// (clear the thread_wts (do in txn -> record) and clear the txn behind (do in coroutine))
// if use part-reexe can all write be aborted? no we just abort the operation after the aborted.
func (t *TXN) Commit() {
	// validation pass then commit the write operation
	write_rds := t.write_records
	for rd, _ := range(*write_rds) {
		rd.Commit(t)
	}
}