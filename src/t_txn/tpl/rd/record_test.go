package rd

/*
go test t_txn/tpl/rd -v
*/


import (
	"t_log"
	"testing"
	// "sync"
	"fmt"
)

func TestCorrect(t *testing.T) {
	t_log.Loglevel = t_log.DEBUG
	// t_log.Loglevel = t_log.PANIC
	r := NewRecord()
	r.DoneWrite(11)
	fmt.Println(r.Write(11))
	r.DoneWrite(11)
	fmt.Println(r.Write(12))
	r.DoneRead(12)
	fmt.Println(r.Read(11))
	fmt.Println(r.Read(12))
	fmt.Println(r.Write(11))
}