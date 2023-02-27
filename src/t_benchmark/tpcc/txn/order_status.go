package Txn

import (
	"t_benchmark/tpcc_l"
	"strconv"
)

// The attrates should be determined before execution
type OsNonDeterminisitcAttr struct {
	
}

type OsVariables struct {
	c_i int
	o_i int
	ol_i int
}

type OrderStatus struct {
	W_ID int
	D_ID int

	// The attrates should be determined before execution
	c_cnt int
	C_IDs *[]int
	o_cnt int
	O_IDs *[]int
	ol_cnt int
	OL_NUMBERs *[]int
	// ----------------
	nd_attr *OsNonDeterminisitcAttr
	vars *OsVariables
	step int // used for coroutine-to-transaction; saving the state of the storage procedure.
} 


func NewOrderStatus() *OrderStatus {

	str := strconv.Itoa
	// For any given terminal, the home warehouse number (W_ID) is constant over the whole measurement interval.
	W_ID := tpcc_l.UniformInt(1, tpcc_l.Warehouse)

	// The district number (D_ID) is randomly selected within [1 ..10] from the home warehouse.
	D_ID := tpcc_l.UniformInt(1, 10)
	// This can be implemented by generating two random numbers x and y within [1 .. 100];
	y := tpcc_l.UniformInt(1, 100)

	var c_cnt int
	var C_IDs *[]int = nil
	if y <= 60 {
		// If y <= 60 a customer last name (C_LAST) is generated according to Clause 4.3.2.3 from a non-uniform random value using the NURand (255,0,999) function. The customer is using his/ her last name and is one of the possibly several customers with that last name.
		// since there is no abort, we find till get an available C_IDs.
		for C_IDs == nil {
			C_LAST := tpcc_l.NURand(255, 0, 999)
			C_IDs = tpcc_l.GetCIDByWDCLast("W_ID" + str(W_ID) + "D_ID" + str(D_ID) + "C_LAST" + str(C_LAST))
		}
		c_cnt = len(*C_IDs)
		
	} else {
		// If y > 60 a non-uniform random customer number (C_ID) is selected using the NURand (1023,1,3000) function. The customer is using his/ her customer number.
		C_ID := tpcc_l.NURand(1023, 1, 3000)
		t := []int{C_ID}
		C_IDs = &t
		c_cnt = 1
	}

	var o_cnt = 0
	C_IDi := c_cnt / 2
	O_IDs := tpcc_l.GetOByWDC("O_W_ID" + str(W_ID) + "O_D_ID" + str(D_ID) + "O_C_ID" + str((*C_IDs)[C_IDi]))

	var O_ID int = 0
	var OL_NUMBERs *[]int = nil
	var ol_cnt int = 0

	if O_IDs != nil {
		o_cnt = len(*O_IDs)

		O_ID = (*O_IDs)[0]
		OL_NUMBERs = tpcc_l.GetOLNByWDO("OL_W_ID" + str(W_ID) + "OL_D_ID" + str(D_ID) + "OL_O_ID" + str(O_ID))
		ol_cnt = len(*OL_NUMBERs)
	}

	// The row in the ORDER table with matching O_W_ID (equals C_W_ID), O_D_ID (equals C_D_ID), O_C_ID (equals C_ID), and with the largest existing O_ID, is selected. 
	

	vars := OsVariables{0, 0, 0}
	return &OrderStatus{W_ID, D_ID, c_cnt, C_IDs, o_cnt, O_IDs, ol_cnt, OL_NUMBERs, nil, &vars, 0}

}


// if preivous is not the read operations, r can be a 'nil'.
// if return nil, the transaction can be committed.
func (os *OrderStatus) Next(r *string) *tpcc_l.Op {
	str := strconv.Itoa
	switch os.step { 
	// A database transaction is started.
	case 0:
		// Case 1, the customer is selected based on customer number: the row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is selected and C_BALAN CE, C_FIRST, C_MIDDLE, and C_LAST are retrieved.
		if os.c_cnt == 1 {
			os.step = 1
			return tpcc_l.Read("C_W_ID" + str(os.W_ID) + "C_D_ID" + str(os.D_ID) + "C_ID" + str((*os.C_IDs)[0]))
		} else {
		// Case 2, the customer is selected based on customer last name: all rows in the CUSTOMER table with matching C_W_ID, C_D_ID and C_LAST are selected sorted by C_FIRST in ascending order.
			c_i := os.vars.c_i 
			os.vars.c_i ++
			if c_i < os.c_cnt {
				if os.vars.c_i == os.c_cnt {
					os.vars.c_i = 0
					os.step = 1
				}
				return tpcc_l.Read("C_W_ID" + str(os.W_ID) + "C_D_ID" + str(os.D_ID) + "C_ID" + str((*os.C_IDs)[c_i]))
			} 
		}
	case 1:
		// The row in the ORDER table with matching O_W_ID (equals C_W_ID), O_D_ID (equals C_D_ID), O_C_ID (equals C_ID), and with the largest existing O_ID, is selected. This is the most recent order placed by that customer. O_ID, O_ENTRY_D, and O_CARRIER_ID are retrieved.
		if os.o_cnt == 0 {
			return nil // there is no order line need to check
		} else {
			o_i := os.vars.o_i 
			os.vars.o_i ++
			if o_i < os.o_cnt {
				if os.vars.o_i == os.o_cnt {
					os.vars.o_i = 0
					os.step = 2
				}
				return tpcc_l.Read("O_W_ID" + str(os.W_ID) + "O_D_ID" + str(os.D_ID) + "O_ID" + str((*os.O_IDs)[o_i]))
			} 
		}
	case 2:
		// All rows in the ORDER-LINE table with matching OL_W_ID (equals O_W_ID), OL_D_ID (equals O_D_ID), and OL_O_ID (equals O_ID) are selected and the corresponding sets of OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, and OL_DELIVERY_D are retrieved.
		ol_i := os.vars.ol_i 
		os.vars.ol_i ++
		if ol_i < os.ol_cnt {
			if os.vars.ol_i == os.ol_cnt {
				os.vars.ol_i = 0
				os.step = 3
			}
			OL_NUMBER := (*os.OL_NUMBERs)[ol_i]
			return tpcc_l.Read("OL_W_ID" + str(os.W_ID) + "OL_D_ID" + str(os.D_ID) + "OL_O_ID" + str((*os.O_IDs)[0]) +
			"OL_NUMBER" + str(OL_NUMBER))
		} 
		case 3:
			return nil
	}
	return nil
}
