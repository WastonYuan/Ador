package main

// go test t_coro/test -bench=. -v -cpu 1
import (
	// "t_log"
	"math/rand"
	"sync"
	"fmt"
	"time"
)


func main() {
	 
	var wg sync.WaitGroup
	core := 32
	run_count := 100000
	const t_count = 5
	t := make(chan int, t_count)
	per_t := run_count / t_count
	start := time.Now()
	for i := 0; i < core; i ++ {
		wg.Add(1)
		go Run(per_t, &wg, t)
	}
	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("time: %v, thread: %v\n", elapsed, t_count)

}

func Run(loop int, wg *sync.WaitGroup, t chan int) {
	t <- 1
	for i := 0 ; i < loop; i ++ {
		rand.Int()
	}
	<- t
	wg.Done()
}