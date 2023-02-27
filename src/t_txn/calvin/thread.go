package calvin

import (
	"fmt"
	"t_log"
)

var exec_cost int = 2

type Stats struct {
	reserve_step int
	read_cnt int
	write_cnt int
	lock_step int
	commit_step int
	commit_cnt int
	step int
	block_step int
}

type Thread struct {
	batch *Batch
	statics *Stats
}


func StatisticsTitle() string {
	return "reserve_step\tread_cnt\twrite_cnt\tlock_step\tcommit_step\tcommit_cnt\tstep\tblock_step"
}

func (t *Thread) StatisticsResult() string {
	return fmt.Sprintf("%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v", t.statics.reserve_step, t.statics.read_cnt, t.statics.write_cnt,
	t.statics.lock_step, t.statics.commit_step, t.statics.commit_cnt, t.statics.step, t.statics.block_step)
}


func NewThread(batch *Batch) *Thread {
	return &Thread{batch, &Stats{}}
}

func (t *Thread) RunFirstPhase() {
	for true {
		coro := t.batch.Pop()
		if coro != nil {
			coro.SetThread(t)
			// t_log.Log(t_log.DEBUG, "txn %v reserve\n", coro.txn.txn_id)
			coro.Reserve()
		} else {
			break
		}
	}

}


func (t *Thread) RunSecondPhase() {
	for true {
		coro := t.batch.Pop()
		if coro != nil {
			// t_log.Log(t_log.DEBUG, "coro %v start\n", coro.GetTxnId())
			coro.SetThread(t)
			// t_log.Log(t_log.DEBUG, "txn %v exec\n", coro.txn.txn_id)
			for ok := coro.IsRunable(); !ok; ok = coro.IsRunable() {
				cc.Sync(t)
			}
			coro.Reset()
			for ok := coro.Execute(); ok; ok = coro.Execute() {
				for i:=0 ; i < exec_cost; i++ {cc.Sync(t)}
			}
			// t_log.Log(t_log.DEBUG, "txn %v commit\n", coro.txn.txn_id)
			coro.Commit()
			t_log.Log(t_log.DEBUG, "coro %v committed\n", coro.GetTxnId())
		} else {
			break
		}
	}
	cc.Exit(t)
}