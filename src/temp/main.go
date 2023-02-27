package main

import (
	"fmt"
	
)


type ReadReq struct {
	// request
	key string // if multi-read the key is the left range.
	s_len int // if single read the len is 0
} 

func main() {
	rq1 := ReadReq{"hello", 5}
	rq2 := ReadReq{"hello", 5}
	m := map[ReadReq]int{}
	m[rq1] = 10
	fmt.Println(m[rq2])
}