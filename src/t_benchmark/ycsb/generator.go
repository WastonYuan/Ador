package ycsb


import (
	"math"
	"math/rand"
	"t_txn"
	"container/list"
	"strconv"
	"t_log"
)

const record_count = 100000
const pre_fix = "r" // if need partition can change this

type Ycsb struct {
	pf *([record_count]float64)
	txn_len int
	scan_len int
	write_rate float64
	read_rate float64
	// scan_rate float64
}

// in general c = 0.1
func NewYCSB(c float64, a float64, txn_len int, scan_len int, write_rate float64, read_rate float64) *Ycsb {

	pf := [record_count]float64{}
	var sum float64 = 0

	for i := 0; i < record_count; i++ {
		rank := float64(i + 1)
		sum = sum +  c / (math.Pow(rank, a))
	}
	for i := 0; i < record_count; i++ {
		rank := float64(i + 1)
		if i == 0 {
			pf[i] = c / (math.Pow(rank, a)) / sum
		} else {
			pf[i] = pf[i-1] + c / (math.Pow(rank, a)) / sum
		}
	}
	return &(Ycsb{&pf, txn_len, scan_len, write_rate, read_rate})
}


type YcsbOPS struct {
	ops *list.List // store *t_txn.OP
	op2ele *map[t_txn.OP](*list.Element) // store the first access of the op
	cur_ele *list.Element
}


func (ops *YcsbOPS) Get() *t_txn.OP {
	if ops.cur_ele != nil {
		return ops.cur_ele.Value.(*t_txn.OP)
	} else {
		return nil
	}
}

func (ops *YcsbOPS) Len() int {
	return ops.ops.Len()
}

func (ops *YcsbOPS) Next() bool {
	ops.cur_ele = ops.cur_ele.Next()
	if ops.cur_ele == nil {
		return false
	} else {
		return true
	}
}

func (ops *YcsbOPS) Reset() {
	ops.cur_ele = ops.ops.Front()
}


func (ops *YcsbOPS) Progress() {
	cur_ele := ops.cur_ele
	// below for introduction (*if doing the rebase test, then release the note*)
	if cur_ele == nil {
		t_log.Log(t_log.INFO, "%v\tok\n", ops.ops.Len())
	} else {
		index := 1
		for e := ops.ops.Front(); e != cur_ele ; e = e.Next() {
			index ++
		}
		t_log.Log(t_log.INFO, "%v\n", index)
	}
	// end
}

func (ops *YcsbOPS) Revert(op *t_txn.OP) {
	ele := (*ops.op2ele)[*op]
	ops.cur_ele = ele
	// below for introduction (*if doing the rebase test, then release the note*)
	// index := 0
	// for e := ops.ops.Front(); e != ele ; e = e.Next() {
	// 	index ++
	// }
	// t_log.Log(t_log.INFO, "%v\n", index)
	// end

}


// not include range query
func (ops *YcsbOPS) ReadWriteSet() (*(map[string]bool), *(map[string]bool)) {
	read_set := map[string]bool{}
	write_set := map[string]bool{}
	l := ops.ops
	for ele := l.Front(); ele != nil; ele = ele.Next() {
		op := ele.Value.(*t_txn.OP)
		if op.Type == t_txn.OP_READ {
			read_set[op.Key] = true
		} else if op.Type == t_txn.OP_WRITE {
			write_set[op.Key] = true
		}
	}
	return &read_set, &write_set
}


func (ops *YcsbOPS) ComplexWriteSet() *(map[string]int) {
	c_write_set := map[string]int{}
	l := ops.ops

	for ele := l.Front(); ele != nil; ele = ele.Next() {
		op := ele.Value.(*t_txn.OP)
		if op.Type == t_txn.OP_WRITE {
			value, ok := c_write_set[op.Key]
			if ok {
				c_write_set[op.Key] = value + 1
			} else {
				c_write_set[op.Key] = 1
			}
		}
	}
	return &c_write_set
}

func (ycsb *Ycsb) NewOPS() t_txn.AccessPtr {
	l_ops := list.New()
	op2ele := map[t_txn.OP](*list.Element){}
	for i := 0; i < ycsb.txn_len; i ++ {
		// key_str
		r_rate := rand.Float64()
		index := 0
		for r_rate > ycsb.pf[index] {
			index ++
		}
		key := pre_fix + strconv.Itoa(index)
		// op type
		var op_type int
		type_rate := rand.Float64()
		if type_rate < ycsb.write_rate {
			op_type = t_txn.OP_WRITE
		} else if type_rate < ycsb.write_rate + ycsb.read_rate {
			op_type = t_txn.OP_READ
		} else {
			op_type = t_txn.OP_SCAN
		}
		var n_op *t_txn.OP
		if op_type == t_txn.OP_SCAN {
			n_op = &(t_txn.OP{key, ycsb.scan_len, op_type})
		} else {
			n_op = &(t_txn.OP{key, 0, op_type})
		}
		ele := l_ops.PushBack(n_op)
		_, ok := op2ele[*n_op]
		if ok == false {
			op2ele[*n_op] = ele
		}
	}
	return &YcsbOPS{l_ops, &op2ele, l_ops.Front()}
	
}


// type AccessPtr interface {
// 	Next() bool
// 	Get() *OP
// 	Len() int
// 	Reset()
// 	// ReadWriteSeq() *([](*OP))
// 	ReadWriteSet() *(map[string]bool)
// 	// WriteSet()
// }


