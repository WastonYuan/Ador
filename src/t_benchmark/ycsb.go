package t_benchmark

import (
	"strconv"
	"math/rand"
	"t_txn"
)

type Ycsb struct {
	prefix string
	average float64
	variance float64 // for control zkew
	txn_len int
	write_rate float64
}

func NewYcsb(p string, a float64, v float64, l int, w float64) *Ycsb {
	// check the parameter
	return &Ycsb{p, a, v, l, w}
}


/*
this method can be run concurency with one ycsb
*/
func (y Ycsb) NewOPS() *t_txn.OPS {
	ops := make([](*(t_txn.OP)), y.txn_len)
	for i := 0; i < y.txn_len; i++ {
		var key string
		var is_write bool
		// generate record
		if rand.Float64() <= y.write_rate {
			is_write = true
		} else { 
			is_write = false
		}
		key = y.prefix + strconv.Itoa(int(rand.NormFloat64() * y.variance +  y.average))
		ops[i] = t_txn.NewOP(key, is_write)
	}
	return t_txn.NewOPS(ops)
}