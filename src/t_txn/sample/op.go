package sample

import (
	"t_util"
	"t_txn"
	// "t_log"
)
/*
use for test coro and the sample of the txn model
*/

type Rand struct {
	r float64
}


func NewRand(r float64) *Rand {
	return &Rand{r}
}


type TXN struct {
	base *Rand 
}

func (r *Rand) NewTXN() *TXN {
	return &TXN{r}
}

func (t *TXN) Write(key string) int {
	if t_util.RandFloat() < t.base.r {
		return t_txn.NEXT
		
	} else {
		return t_txn.AGAIN
	}
}


func (t *TXN) Read(key string) int {
	if t_util.RandFloat() < t.base.r {
		return t_txn.NEXT
		
	} else {
		return t_txn.RERUN
	}
}

func (t *TXN) Reset() {

}



