package t_util

/*
go test t_util -v
*/


import (
	"testing"
	// "fmt"
	// "time"
	// "t_util"
)



func TestWrite(t *testing.T) {
	re := InitFile("hello.log")
	re.Write("what the fuck 666\n")
	re.Write("what the fuck 777\n")
	re.Write("what the fuck 888\n")
	re.Write("what the fuck 999\n")

}