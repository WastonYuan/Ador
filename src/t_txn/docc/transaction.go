package docc

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


	return &TXN{txn_id, coro, o,
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
read must success
*/
func (t *TXN) SingleRead(key string) {
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
}


/*
abort must success
the txn (coroutine)
*/
func (t *TXN) Abort() {
	// first clear the thread_wts of the writing record.
	for r, _ := range(*t.write_records) {
		r.Abort(t)
	}
	
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
		// first check the single read.
		if read_req.IsSingleRead() {
			key := read_req.Key
			if (*t.key2rts)[key] == -1 {
				// if rts == -1, then check the record is not exist or the record is not available
				r := base.Search(key)
				if r == nil {
					continue
				} else {
					r.Validate(-1)
				}
			} else {
				// check normal conflict
				rts := (*t.key2rts)[key]
				rd := (*t.key2record)[key]
				res := rd.Validate(rts)
				if res == false {
					return key, 0, false, false
				} else {
					continue
				}
			}
		// then check the multi read
		} else {
			return "", 0, false, true
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