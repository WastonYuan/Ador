
package common


import (
	"t_benchmark"
	"t_txn"
	"strings"
	"strconv" 
	"t_benchmark/tpcc"
	"fmt"
)



const (
	TPCC    = -1
	YCSB 	= 0
)

type Benchmark struct {
	b_type int
	
	warehouse int
	np_rate float64

	skew float64
	write_rate float64


	epoch_size int
} 

func NewTPCC(warehouse int, np_rate float64, epoch_size int) *Benchmark {

	return &(Benchmark{TPCC, warehouse, np_rate, 0, 0, epoch_size})
	
}


func NewYCSB(skew float64, write_rate float64, epoch_size int) *Benchmark {
	return &(Benchmark{YCSB, 0, 0, skew, write_rate, epoch_size})
}


func (b *Benchmark) Encode() string {
	if b.b_type == TPCC {
		res := "t"
		w_s := strconv.Itoa(b.warehouse)
		res = res + w_s + ";"
		n_s := fmt.Sprintf("%f", b.np_rate)
		res = res + n_s
		return res
	} else {

		a := float64(1000000)
		// write_rate := float64(0.9)

		v := float64(1/b.skew)
		t_len := 20
		// average variance len write_rate
		ycsb := t_benchmark.NewYcsb("t", a, v, t_len, b.write_rate)
		t_count := b.epoch_size
		opss := make([](t_txn.AccessPtr), t_count)
		
		// for test hang
		// degree := make(chan int, 3)
		
		/* generate txn and reorder(or not) */
		for i := 0; i < t_count; i++ {
			ops := ycsb.NewOPS() // actually read write sequence
			opss[i] = ops
		}
		// generate opss ok
		var res = "y"
		for i := 0; i < len(opss); i++ {
			ops := opss[i]
			ops.Reset()
			for true {
				op := ops.Get()
				if op != nil {
					// t_log.Log(t_log.INFO, "do read write")
					res = res + op.Key
					if op.Is_write == true {
						res = res + "|1,"
					} else {
						res = res + "|0,"
					}
				} else {
					res = res + ";"
					break
				}
				ops.Next()
			}
		}
		return res
	}
}



func Decode(s string) [](t_txn.AccessPtr) {
	bench_type := s[0]
	s = s[1:]
	if bench_type == 'y' {
		v := strings.Split(s, ";")
		v = v[:len(v)-1]
		// fmt.Println(v)
		opss := make([](t_txn.AccessPtr), len(v))
		for i := 0; i < len(v); i++ {
			s_ops := strings.Split(v[i], ",")
			s_ops = s_ops[:len(s_ops) - 1]
			// fmt.Println(s_ops)
			ops := make([](*(t_txn.OP)), len(s_ops))
			for j := 0; j < len(s_ops); j ++ {
				op := strings.Split(s_ops[j], "|")
				key := op[0]
				var is_write bool
				if op[1] == "1" {
					is_write = true
				} else {
					is_write = false
				}
				ops[j] = t_txn.NewOP(key, is_write)
			}
			// fmt.Println(ops)
			acc := t_txn.NewOPS(ops)
			opss[i] = acc
		}
		return opss
		
	} else {
		v := strings.Split(s, ";")
		// fmt.Println(v)
		w, _ := strconv.Atoi(v[0])
		np_str := v[1]
		np_rate, _ := strconv.ParseFloat(np_str, 64) 
		// {
		// 	fmt.Printf("%T, %v\n", np_str, np_str)
		// }
		// fmt.Println(w, np_rate, np_str)
		// tpcc_bench := tpcc.NewTPCC(int(w) , np_rate)
		if np_rate == 0 {
			np_rate = 0.5
		}
		tpcc_bench := tpcc.NewTPCC(int(w) , np_rate)

		const t_count = 1000
		opss := make([](t_txn.AccessPtr), t_count)
	

		/* generate txn and reorder(or not) */
		for i := 0; i < t_count; i++ {
			ops := tpcc_bench.NewOPS() // actually read write sequence

			opss[i] = ops
		}
		return opss
		
	}
}