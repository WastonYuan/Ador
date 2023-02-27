package t_txn

import(
	// "fmt"
	// "time"
	// "t_util"
	// "strconv"
	// // "t_log"
)


const (
	OP_WRITE = -1
	OP_READ = 0
	OP_SCAN = 1
)

/*
if not scan Len = 0
*/
type OP struct {
	Key string
	Len int
	Type int
}

func NewOP(key string, s_len int, op_type int) *OP {
	return &OP{key, s_len, op_type}
}



type AccessPtr interface {
	Next() bool
	Get() *OP
	Len() int
	Progress()
	Reset()
	Revert(*OP)
	ReadWriteSet() (*(map[string]bool), *(map[string]bool))
	ComplexWriteSet() *(map[string]int)
}