package t_index

/*
go test t_index -v array_test.go
*/

import (
	"testing"
	"t_txn/tpl/rd"
	"fmt"
	// "t_util"
	// "sync"
	// "strconv"
	// "math/rand"
	// "time"
	// "t_util"
)

/* the record should be set to 2pl */
/* test write can be read in same txn */

func TestBasic(t *testing.T) {

	arr := NewArray(5)
	r := rd.NewRecord()
	arr.Insert("5555", r)
	arr.Insert("3333", r)
	v := arr.Search("3333")

}