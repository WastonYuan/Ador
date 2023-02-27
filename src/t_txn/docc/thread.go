package docc

import (
	"container/list"
	"fmt"
	"runtime"
	"t_log"
)

/*
thread has a pointer of current running coro
*/

var exec_2_cost = 1
var exec_cost int = 2
var validate_cost int = 5

type Stats struct {
	read_cnt int
	write_cnt int
	major_read_cnt int
	major_write_cnt int
	commit_cnt int 
	abort_cnt int
	rebase_cnt int
	conflict_cnt int
	phantom_cnt int
	step int
	block_step int
}

type Thread struct {
	ThreadID int // from 0 to n
	coros *list.List
	cur_ele *list.Element // if == nil then all txn in the thread has been completed.
	statics *Stats
}


func StatisticsTitle() string {
	return "read_cnt\twrite_cnt\tmajor_read_cnt\tmajor_write_cnt\tcommit_cnt\tabort_cnt\trebase_cnt\tconflict_cnt\tphantom_cnt\tstep\tblock_step"
}

func (t *Thread) StatisticsResult() string {
	return fmt.Sprintf("%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v", t.statics.read_cnt, t.statics.write_cnt, t.statics.major_read_cnt, t.statics.major_write_cnt, 
	t.statics.commit_cnt, t.statics.abort_cnt, t.statics.rebase_cnt, t.statics.conflict_cnt, t.statics.phantom_cnt, t.statics.step, t.statics.block_step)
}


func NewThread(thread_id int) *Thread {
	return &Thread{thread_id, list.New(), nil, &Stats{}}
}


func (t *Thread) PushCoro(coro *Coroutine) {
	t.coros.PushBack(coro)
	coro.SetThread(t)
}



func (t *Thread) Run() {
	
	
	// each thread just run onece each batch (the first coro is the head of the coros)
	t.cur_ele = t.coros.Front()
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
			for _, ok := front_coro.Exec(true); ok; _, ok = front_coro.Exec(true) {
				// cc.Sync(t)
				for i :=0 ;i < exec_cost; i++ {cc.Sync(t)}
			}
			ok := front_coro.Validate()
			for i :=0 ;i < validate_cost; i++ {cc.Sync(t)}
			
			// Sync()
			if ok == false {
				front_coro.Abort()
			}
			for _, ok := front_coro.Exec(true); ok; _, ok = front_coro.Exec(true) {
				// cc.Sync(t)
				for i :=0 ;i < exec_2_cost; i++ {cc.Sync(t)}
			}
			t_log.Log(t_log.DEBUG, "coro_%v of thread_%v is committed, validate_ok:%v, abort_cnt:%v\n", front_coro.GetTxnId(), t.ThreadID, ok, abort_cnt)
			front_coro.Commit()
			// for i :=0 ;i < 5; i++ {cc.Sync(t)}
			t.coros.Remove(front_ele)
		} else {
			_, ok := front_coro.Exec(false)
			if ok == true {
				for i :=0 ;i < exec_cost; i++ {cc.Sync(t)}
			} else {
				t.statics.block_step ++
				cc.Sync(t)
			}
		}
		// yield the thread
		runtime.Gosched()
	}
	cc.Exit(t)
}