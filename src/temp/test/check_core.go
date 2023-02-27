package main

import (
	"runtime"
	"fmt"
)


func main() {
	fmt.Println(runtime.NumCPU())
	fmt.Println(runtime.GOMAXPROCS(0))

}