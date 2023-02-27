package gria

/*
go test t_txn/gria -v
*/


import (
	"t_log"
	"testing"
	// "sync"
	"fmt"
)

func TestOP(t *testing.T) {
	t_log.Loglevel = t_log.DEBUG
	// t_log.Loglevel = t_log.PANIC
	db := New(2)
	txn_1_1 := db.NewTXN(1, nil)
	txn_1_1.Init(1)
	txn_2_2 := db.NewTXN(2, nil)
	txn_2_2.Init(2)
	txn_3_1 := db.NewTXN(3, nil)
	txn_3_1.Init(1)
	txn_4_2 := db.NewTXN(4, nil)
	txn_4_2.Init(2)
	txn_5_1 := db.NewTXN(5, nil)
	txn_5_1.Init(1)

	// =============================
	// txn_1_1.Write("aaa")
	txn_2_2.Read("aaa")
	txn_2_2.Write("aaa")
	txn_3_1.Read("aaa")
	txn_3_1.Write("aaa")
	txn_4_2.Write("aaa")
	txn_5_1.Read("aaa")
	fmt.Println(txn_5_1.PrintReadVersins())
	txn_5_1.Write("aaa")
	txn_5_1.Read("aaa")
	fmt.Println(txn_5_1.PrintReadVersins())
	


	fmt.Println(db.PrintRecordList("aaa"))
	fmt.Println(db.PrintRecordList("aaa"))

	// read validation
	fmt.Println(txn_3_1.ReadValidation())
	fmt.Println(txn_5_1.ReadValidation())
	fmt.Println(txn_2_2.ReadValidation())
	
}



