package common

/*
go test t_distributed/common -v
*/


import (
	"testing"
	"fmt"
	// "time"
	// "t_util"
)



func TestParrel(t *testing.T) {
	bench := NewYCSB(0.001, 0.5)
	s := bench.Encode()
	fmt.Println(s)
	fmt.Println(Decode(s))

	fmt.Println()
	// fmt.Println()
	// opss2 := Decode(s)
	// s2 := Encode(opss2)
	// fmt.Println(s2)
	// fmt.Println(s == s2)
	// fmt.Println(s == s2 + " ")

	// test tpcc
	bench2 := NewTPCC(16, 0.5)
	s3 := bench2.Encode()
	fmt.Println(s3)
	fmt.Println()
	fmt.Println(Decode(s3))

}