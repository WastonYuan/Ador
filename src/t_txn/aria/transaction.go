package aria

import (
	"fmt"
)





type Scan struct {
	start_key string
	loop int
}


type TXN struct {
	txn_id int
	read_map *(map[string](*Record)) // save read write map for commit validate (one to commit this read/write map must be consistency)
	write_map *(map[string](*Record))

	base *Aria
}



/*
mainly use for internal test
*/
func (t *TXN) GetReadString() string {
	var res string
	for key, r := range (*t.read_map) {
		res = res + fmt.Sprintf("[%v: %v] ", key, r.Get_min_wid())
	}
	return res
}

func (t *TXN) GetWriteString() string {
	var res string
	for key, r := range (*t.write_map) {
		res = res + fmt.Sprintf("[%v: %v] ", key, r.Get_min_wid())
	}
	return res
}

func (aria *Aria) NewTXN(txn_id int) *TXN {
	r_map := map[string](*Record){}
	w_map := map[string](*Record){}
	return &(TXN{txn_id, &r_map, &w_map, aria})
}





// first phase write always ok
func (t *TXN) Write(key string) bool {
	base := t.base
	r := base.quickGetOrInsert(key, NewRecord())
	// save this op
	(*(t.write_map))[key] = r
	r.Write(t.txn_id)
	return true
}

//first phase read always ok
func (t *TXN) Read(key string) bool {
	base := t.base
	r := base.quickGetOrInsert(key, NewRecord())
	(*(t.read_map))[key] = r
	r.Read(t.txn_id)
	return true
}



func (t *TXN) Reset() {
	t.read_map = &map[string](*Record){}
	t.write_map = &map[string](*Record){} 
}

/*
exec when all write read return true
if read write return false onece Commit must be false (and no need to do this to validate again)
if read write all return true there need to use Commit to verify it will be abort or not
if commit failed or read/write failed the txn should be exec in next batch with same order
*/
func (t *TXN) Commit() bool { // commit is run in 
	// validate read

	rm := t.read_map
	raw := false
	war := false
	for _, r := range (*rm) {
		// any less than txn_id measn WAR OR WAW all need to abort
		// if the record do not write by any txn will this validate will ok
		// no raw or waw 
		if r.Get_min_wid() < t.txn_id && r.Get_min_wid() != -1 {
			raw = true
		}
	}
	// validate write
	wm := t.write_map
	for _, r := range (*wm) {
		if r.Get_min_wid() < t.txn_id && r.Get_min_wid() != -1 {
			return false
		}
		if r.Get_min_rts() < t.txn_id {
			war = true
		}
	}
	// (reordering mechanism)
	if raw == false || war == false {
		return true
	} else {
		return false 
	}

}