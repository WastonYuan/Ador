
package math

import (
	"t_util"
)

func NURand(A int, x int, y int) int {
	// return (uniform_dist(0, A) | uniform_dist(x, y)) % (y - x + 1) + x;
	return (t_util.RandIntFromTo(0, A) | t_util.RandIntFromTo(x, y)) % (y - x + 1) + x;
}

func RandLastName(n int) string {

	s1 := Customer_last_names[n / 100]
	s2 := Customer_last_names[n / 10 % 10]
	s3 := Customer_last_names[n % 10]

    return s1 + s2 + s3;
}


var Customer_last_names = []string{"BAR", "OUGHT", "ABLE",  "PRI",   "PRES", "ESE", "ANTI",  "CALLY", "ATION", "EING"}

	


