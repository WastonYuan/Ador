package math

/*
go test t_benchmark/tpcc/math -v
*/


import (
	"testing"
	"fmt"
	// "sync"
)

func TestCorrect(t *testing.T) {

	fmt.Println(NURand(1023, 1, 3000))
	fmt.Println(RandLastName(123))
	
}