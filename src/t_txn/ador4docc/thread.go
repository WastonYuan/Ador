package ador4docc

import (
	"container/list"
	"fmt"
	"t_log"
	"runtime"
	"math/rand"
	"math"
)

var validate_cost int = 2

/*
thread has a pointer of current running coro
*/

type Stats struct {
	read_cnt int
	write_cnt int
	commit_cnt int 
	abort_cnt int
	rebase_cnt int
	conflict_cnt int
	phantom_cnt int
	prefetch_cnt int

	step int
	block_step int
}

type Thread struct {
	ThreadID int // from 0 to n
	coros *list.List
	cur_pref_ele *list.Element
	statics *Stats
}


func StatisticsTitle() string {
	return "read_cnt\twrite_cnt\tcommit_cnt\tabort_cnt\trebase_cnt\tconflict_cnt\tphantom_cnt\tprefetch_cnt\tstep\tblock_step"
}

func (t *Thread) StatisticsResult() string {
	return fmt.Sprintf("%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v", t.statics.read_cnt, t.statics.write_cnt, t.statics.commit_cnt, t.statics.abort_cnt, t.statics.rebase_cnt, t.statics.conflict_cnt, t.statics.phantom_cnt, 
	t.statics.prefetch_cnt, t.statics.step, t.statics.block_step)
}


func NewThread(thread_id int) *Thread {
	return &Thread{thread_id, list.New(), nil, &Stats{}}
}
 

func (t *Thread) PushCoro(coro *Coroutine) {
	t.coros.PushBack(coro)
	coro.SetThread(t)
}


func (t *Thread) run_op() int {
	var run_op int
	if global_thread_cnt <= core_cnt {
		// parallel / 2
		run_op = int(10 * math.Sqrt(float64(global_thread_cnt)))
		// run_op = 5  * global_thread_cnt / 2
	} else {
		// parallel / 2 to each thread
		run_op = int(10 * math.Sqrt(float64(global_thread_cnt))) * core_cnt / global_thread_cnt
	}
	run_op = int(rand.NormFloat64() * float64(run_op) * 0.3 + float64(run_op))
	if run_op < 1 {
		run_op = 1
	}
	return run_op
}


func (t *Thread) CoroDis(coro1 *Coroutine, coro2 *Coroutine) int {
	txn_id1 := coro1.GetTxnId()
	txn_id2 := coro2.GetTxnId()
	return (txn_id2 - txn_id1) / global_thread_cnt
}


func (t *Thread) Run() {
	
	// each thread just run onece each batch (the first coro is the head of the coros)
	t.cur_pref_ele = t.coros.Front()
	// run coro one by one
	for t.coros.Len() > 0 {
		
		// first check the first coro is committable. If it is committable then validate, and then run the rest of the first txn.
		front_ele := t.coros.Front()
		front_coro := front_ele.Value.(*Coroutine)
		abort_cnt := 0 // for debug
		if front_coro.IsCommitable() {
			// limit coro run 
			// must run until it commit
			// for _, ok := front_coro.Exec(true); ok; _, ok = front_coro.Exec(true) {}
			// t_log.Log(t_log.DEBUG, "coro_%v of thread_%v begin validate\n", front_coro.GetTxnId(), t.ThreadID)
			for cost, ok := front_coro.Exec(); ok; cost, ok = front_coro.Exec() {
				for i :=0 ;i < cost; i++ {cc.Sync(t)}
			}
			front_coro.DoneExec()
			ok := front_coro.Validate()
			// t_log.Log(t_log.DEBUG, "coro_%v of thread_%v validate ok\n", front_coro.GetTxnId(), t.ThreadID)
			for i :=0 ;i < validate_cost; i++ {cc.Sync(t)}
			if ok == false {
				front_coro.Abort()
			}
			for cost, ok := front_coro.Exec(); ok; cost, ok = front_coro.Exec() {
				for i :=0 ;i < cost; i++ {cc.Sync(t)}
			}
			for i :=0 ;i < validate_cost; i++ {cc.Sync(t)}
			t_log.Log(t_log.DEBUG, "coro_%v of thread_%v is committed, validate_ok:%v, abort_cnt:%v\n", front_coro.GetTxnId(), t.ThreadID, ok, abort_cnt)
			front_coro.Commit()
			t.coros.Remove(front_ele)
			if t.cur_pref_ele == front_ele {
				t.cur_pref_ele = t.coros.Front()
			}
		} else {
			if front_coro.IsRunable() && !(front_coro.IsExecDone()) { // in the front of the coros
				if t.cur_pref_ele == front_ele {
					t.cur_pref_ele = front_ele.Next()
				}
				cost, ok := front_coro.Exec()
				if ok {
					// cc.Sync(t)
					for i :=0 ;i < cost; i++ {cc.Sync(t)}
				} else {
					front_coro.DoneExec()
				}
			} else {
				cc.Sync(t)
				t.statics.block_step ++
			}
		}
		// yield the thread
		runtime.Gosched()
	}
	cc.Exit(t)
}


