package tpce


import (
	"math/rand"
	"container/list"
	"t_txn"
	"fmt"
)

const (
	NEW_ORDER = 0
	PAYMENT = 1
)


const customer_cnt = 16
const district_cnt = 8 // if need partition can change this

var write_rate = [...]float64{0.3, 0.1} 
var txn_len = [...]int{12, 8}

type Tpcc struct {
	warehouse_cnt int
	// scan_rate float64
}

// in general c = 0.1
func NewTPCC(warehouse_cnt int) *Tpcc {

	return &Tpcc{warehouse_cnt}
}


type TpccOPS struct {
	ops *list.List // store *t_txn.OP
	op2ele *map[t_txn.OP](*list.Element)
	cur_ele *list.Element
}


func (ops *TpccOPS) Get() *t_txn.OP {
	if ops.cur_ele != nil {
		return ops.cur_ele.Value.(*t_txn.OP)
	} else {
		return nil
	}
}

func (ops *TpccOPS) Len() int {
	return ops.ops.Len()
}

func (ops *TpccOPS) Next() bool {
	ops.cur_ele = ops.cur_ele.Next()
	if ops.cur_ele == nil {
		return false
	} else {
		return true
	}
}

func (ops *TpccOPS) Reset() {
	ops.cur_ele = ops.ops.Front()
}

/*omit*/
func (ops *TpccOPS) Progress() {
	return
}


func (ops *TpccOPS) Revert(op *t_txn.OP) {
	ele := (*ops.op2ele)[*op]
	ops.cur_ele = ele
}

// not include range query
func (ops *TpccOPS) ReadWriteSet() (*(map[string]bool), *(map[string]bool)) {
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


func (ops *TpccOPS) ComplexWriteSet() *(map[string]int) {
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

func (tpcc *Tpcc) NewOPS(txn_type int) t_txn.AccessPtr {
	l_ops := list.New()
	op2ele := map[t_txn.OP](*list.Element){}
	for i := 0; i < txn_len[txn_type]; i ++ {

		w_id := int(rand.Float64() * float64(tpcc.warehouse_cnt))
		d_id := int(rand.Float64() * float64(district_cnt))
		c_id := int(rand.Float64() * float64(customer_cnt))

		key := fmt.Sprintf("%v_%v_%v", w_id, d_id, c_id)
		// key_str
		is_write := rand.Float64() < write_rate[txn_type]

		var n_op *t_txn.OP
		if is_write {
			n_op = &(t_txn.OP{key, 0, t_txn.OP_WRITE})
		} else {
			n_op = &(t_txn.OP{key, 0, t_txn.OP_READ})
		}
		ele := l_ops.PushBack(n_op)
		_, ok := op2ele[*n_op]
		if ok == false {
			op2ele[*n_op] = ele
		}
	}
	return &TpccOPS{l_ops, &op2ele, l_ops.Front()}
	
}



