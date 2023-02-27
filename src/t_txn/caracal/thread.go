package caracal

import (
	"fmt"
	"t_log"
)

var exec_cost int = 3
var scan_cost int = 3
var scan_per int = 2

type Stats struct {
	init_step_cnt int
	read_cnt int
	write_cnt int
	read_version_cnt int
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
	return "init_step_cnt\tread_cnt\twrite_cnt\tread_version_cnt\tcommit_step\tcommit_cnt\tstep\tblock_step"
}

func (t *Thread) StatisticsResult() string {
	return fmt.Sprintf("%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v", t.statics.init_step_cnt, t.statics.read_cnt, t.statics.write_cnt, t.statics.read_version_cnt,
	t.statics.commit_step, t.statics.commit_cnt, t.statics.step, t.statics.block_step)
}


func NewThread(batch *Batch) *Thread {
	return &Thread{batch, &Stats{}}
}

func (t *Thread) RunFirstPhase() {
	for true {
		coro := t.batch.Pop()
		if coro != nil {
			coro.SetThread(t)
			coro.InstallVersion()
		} else {
			break
		}
	}

}


func (t *Thread) RunSecondPhase() {
	for true {
		coro := t.batch.Pop()
		if coro != nil {
			t_log.Log(t_log.DEBUG, "coro %v start\n", coro.GetTxnId())
			coro.Reset()
			coro.SetThread(t)
			for ok := coro.Execute(); ok; ok = coro.Execute() {}
			coro.Commit()
			t_log.Log(t_log.DEBUG, "coro %v ok\n", coro.GetTxnId())
		} else {
			break
		}
	}
	cc.Exit(t)
}


func (t *Thread) SetFirstPhaseStats(n int) {
	t.statics.init_step_cnt = n
}