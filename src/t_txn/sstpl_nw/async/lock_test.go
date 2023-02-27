package async

/*
go test t_txn/nw_tpl/async -v
*/

import (
	"testing"
)


func TestCorrect(t *testing.T) {
	
	am := NewAsyncMutex()

	if am.Lock(1) != true {
		t.Errorf("Test error")
	}
	if am.RLock(2) != false {
		t.Errorf("Test error")
	}
	if am.Lock(2) != false {
		t.Errorf("Test error")
	}
	if am.RLock(2) != false {
		t.Errorf("Test error")
	}
	if am.RLock(1) != true {
		t.Errorf("Test error")
	}
	am.RUnlock(1)
	if am.RLock(1) != true {
		t.Errorf("Test error")
	}
	am.Unlock(1)
	if am.RLock(2) != true {
		t.Errorf("Test error")
	}
	if am.RLock(2) != true {
		t.Errorf("Test error")
	}
	am.RUnlock(1)
	if am.Lock(2) != true {
		t.Errorf("Test error")
	}
	if am.Lock(2) != true {
		t.Errorf("Test error")
	}
	if am.Lock(2) != true {
		t.Errorf("Test error")
	}
	if am.Lock(1) != false {
		t.Errorf("Test error")
	}
	am.RUnlock(2)
	am.Unlock(2)

	if am.Lock(1) != true {
		t.Errorf("Test error")
	}
}


func TestPrallel(t *testing.T) {

	am := NewAsyncMutex()
	for i := 0; i < 100; i ++ {
		go func(txn_id int) {
			am.Lock(txn_id)
			am.RLock(txn_id)
			am.Unlock(txn_id)
			am.RUnlock(txn_id)
		}(i)
	}
}