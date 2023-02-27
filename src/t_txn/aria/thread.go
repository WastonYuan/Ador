package aria

import (
	// "container/list"
	"fmt"
	// "t_log"
)

type Stats struct {
	read_cnt int
	write_cnt int
	commit_rw_step_cnt int
	commit_scan_step_cnt int
	commit_cnt int
	abort_cnt int
	exec_txn_cnt int
}

type Thread struct {
	batch *SubBatch
	statics *Stats
}


func StatisticsTitle() string {
	return "read_cnt\twrite_cnt\tcommit_rw_step_cnt\tcommit_scan_step_cnt\tcommit_cnt\tabort_cnt\texec_txn_cnt"
}

func (t *Thread) StatisticsResult() string {
	return fmt.Sprintf("%v\t%v\t%v\t%v\t%v\t%v\t%v", t.statics.read_cnt, t.statics.write_cnt, t.statics.commit_rw_step_cnt, t.statics.commit_scan_step_cnt,
	t.statics.commit_cnt, t.statics.abort_cnt, t.statics.exec_txn_cnt)
}


func (t *Thread) Reset() {
	t.statics = &Stats{}
}

func NewThread(b *SubBatch) *Thread {
	return &Thread{b, &Stats{}}
}


func (t *Thread) RunFirstPhase() {
	 // check state
	 for true {
		ele := t.batch.Assign()
		// t_log.Log(t_log.DEBUG, "run first phase\n")
		if ele != nil {
			// t_log.Log(t_log.DEBUG, "run first phase\n")
			coro := ele.Value.(*Coroutine)
			coro.SetThread(t)
			coro.Execution()
			t.statics.exec_txn_cnt = t.statics.exec_txn_cnt + 1
		} else {
			return
		}
	}
}

func (t *Thread) RunSecondPhase() {
	// check state
   for true {
	   ele := t.batch.Assign()
	   if ele != nil {
			coro := ele.Value.(*Coroutine)
		    coro.SetThread(t)
		    ok := coro.Commit()
		    if ok == true {
			    t.batch.Remove(ele)
		    } else {
			    coro.Reset()
		    }
	   } else {
		   return 
	   }
   }
}




