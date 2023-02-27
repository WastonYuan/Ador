package Txn

import (
	"t_benchmark/tpcc_l"
	"strconv"
	"fmt"
)

// The attrates should be determined before execution
type DNonDeterminisitcAttr struct {
	
}

type DVariables struct {
	dno_i int
	ol_i int
	
}

type Delivery struct {
	W_ID int
	
	// non-deterministic attr
	d_cnt int
	Del_NO_O_IDs *([]int)
	Del_NO_D_IDs *([]int)
	Del_NO_C_IDs *([]int)
	ol_cnt int
	OL_NUMBERs *([]int)
	OL_D_IDs *([]int)
	OL_O_IDs *([]int)
	// ----------------
	nd_attr *DNonDeterminisitcAttr
	vars *DVariables
	step int // used for coroutine-to-transaction; saving the state of the storage procedure.
} 


func NewDelivery() *Delivery {

	str := strconv.Itoa
	// For a given warehouse number (W_ID)
	W_ID := tpcc_l.UniformInt(1, tpcc_l.Warehouse)

	var Del_NO_O_IDs []int
	var Del_NO_D_IDs []int
	var Del_NO_C_IDs []int
	var OL_NUMBERs []int
	var OL_D_IDs []int
	var OL_O_IDs []int

	var d_cnt int = 0
	var ol_cnt int = 0

	for D_ID := 1; D_ID <= 10; D_ID ++ {
		NOOs := tpcc_l.GetNOOByWD("NO_W_ID" + str(W_ID) + "NO_D_ID" + str(D_ID))
		if NOOs != nil {
			// The row in the NEW-ORDER table with matching NO_W_ID (equals W_ID) and NO_D_ID (equals D_ID) and with the lowest NO_O_ID value is selected.
			NO_O_ID := (*NOOs)[0]
			// The selected row in the NEW-ORDER table is deleted (update the secondary index).
			(*NOOs) = (*NOOs)[1:]
			if len(*NOOs) == 0 {
				tpcc_l.RemoveNOO("NO_W_ID" + str(W_ID) + "NO_D_ID" + str(D_ID))
			}
			C_ID := tpcc_l.GetCByWDO("O_W_ID" + str(W_ID) + "O_D_ID" + str(D_ID) + "O_C_ID" + str(NO_O_ID))
			Del_NO_O_IDs = append(Del_NO_O_IDs, NO_O_ID)
			Del_NO_D_IDs = append(Del_NO_D_IDs, D_ID)
			Del_NO_C_IDs = append(Del_NO_C_IDs, C_ID)
			OL_NUMBERs_part := tpcc_l.GetOLNByWDO("OL_W_ID" + str(W_ID) + "OL_D_ID" + str(D_ID) + "OL_O_ID" + str(NO_O_ID))
			if OL_NUMBERs_part == nil {
				fmt.Println("OL_W_ID" + str(W_ID) + "OL_D_ID" + str(D_ID) + "OL_O_ID" + str(NO_O_ID))
			}
			for _, OL_NUMBER := range(*OL_NUMBERs_part) {
				OL_NUMBERs = append(OL_NUMBERs, OL_NUMBER)
				OL_D_IDs = append(OL_D_IDs, D_ID)
				OL_O_IDs = append(OL_O_IDs, NO_O_ID)
			}

			d_cnt = len(Del_NO_O_IDs)
			ol_cnt = len(OL_NUMBERs)
		}
	}
	

	vars := DVariables{0, 0}

	return &Delivery{W_ID, d_cnt, &Del_NO_O_IDs, &Del_NO_D_IDs, &Del_NO_C_IDs, ol_cnt, &OL_NUMBERs, &OL_D_IDs, &OL_O_IDs, nil, &vars, 0}

}


// if preivous is not the read operations, r can be a 'nil'.
// if return nil, the transaction can be committed.
func (d *Delivery) Next(r *string) *tpcc_l.Op {
	str := strconv.Itoa
	switch d.step { 
		// A database transaction is started.
		case 0:
			// The row in the NEW-ORDER table with matching NO_W_ID (equals W_ID) and NO_D_ID (equals D_ID) and with the lowest NO_O_ID value is selected. This is the oldest undelivered order of that district. NO_O_ID, the order number, is retrieved. 
			dno_i := d.vars.dno_i 
			d.vars.dno_i ++
			if dno_i < d.d_cnt {
				if d.vars.dno_i == d.d_cnt {
					d.vars.dno_i = 0
					d.step = 1
				}
				D_ID := (*d.Del_NO_D_IDs)[dno_i]
				O_ID := (*d.Del_NO_O_IDs)[dno_i]
				return tpcc_l.Read("NO_W_ID" + str(d.W_ID) + "NO_D_ID" + str(D_ID) + "NO_O_ID" + str(O_ID))
			}
			// fallthrough
			// if there is no new order in all strict then no nl and then no op need to handle.
		
		case 1:
			// The selected row in the NEW-ORDER table is deleted.
			dno_i := d.vars.dno_i 
			d.vars.dno_i ++
			if dno_i < d.d_cnt {
				if d.vars.dno_i == d.d_cnt {
					d.vars.dno_i = 0
					d.step = 2
				}
				D_ID := (*d.Del_NO_D_IDs)[dno_i]
				O_ID := (*d.Del_NO_O_IDs)[dno_i]
				return tpcc_l.Write("NO_W_ID" + str(d.W_ID) + "NO_D_ID" + str(D_ID) + "NO_O_ID" + str(O_ID))
			}
		
		case 2:
			// The row in the ORDER table with matching O_W_ID (equals W_ ID), O_D_ID (equals D_ID), and O_ID (equals NO_O_ID) is selected, O_C_ID, the customer number, is retrieved,
			dno_i := d.vars.dno_i 
			d.vars.dno_i ++
			if dno_i < d.d_cnt {
				if d.vars.dno_i == d.d_cnt {
					d.vars.dno_i = 0
					d.step = 3
				}
				D_ID := (*d.Del_NO_D_IDs)[dno_i]
				O_ID := (*d.Del_NO_O_IDs)[dno_i]
				return tpcc_l.Read("O_W_ID" + str(d.W_ID) + "O_D_ID" + str(D_ID) + "O_ID" + str(O_ID))
			}

		case 3:
			// , and O_CARRIER_ID is updated.
			dno_i := d.vars.dno_i 
			d.vars.dno_i ++
			if dno_i < d.d_cnt {
				if d.vars.dno_i == d.d_cnt {
					d.vars.dno_i = 0
					d.step = 4
				}
				D_ID := (*d.Del_NO_D_IDs)[dno_i]
				O_ID := (*d.Del_NO_O_IDs)[dno_i]
				return tpcc_l.Write("O_W_ID" + str(d.W_ID) + "O_D_ID" + str(D_ID) + "O_ID" + str(O_ID))
			}

		case 4:
			// All rows in the ORDER-LINE table with matching OL_W_ID (equals O_W_ID), OL_D_ID (equals O_D_ID), and OL_O_ID (equals O_ID) are selected. All OL_DELIVERY_D, the delivery dates, are updated to the current system time as returned by the operating system
			ol_i := d.vars.ol_i 
			d.vars.ol_i ++
			if ol_i < d.ol_cnt {
				if d.vars.ol_i  == d.ol_cnt {
					d.vars.ol_i  = 0
					d.step = 5
				}
				D_ID := (*d.OL_D_IDs)[ol_i]
				O_ID := (*d.OL_O_IDs)[ol_i]
				OL_NUMBER := (*d.OL_NUMBERs)[ol_i]
				return tpcc_l.Write("OL_W_ID" + str(d.W_ID) + "OL_D_ID" + str(D_ID) + "OL_O_ID" + str(O_ID) + 
				"OL_NUMBER" + str(OL_NUMBER))
			}
		case 5:
			// and the sum of all OL_AMOUNT is retrieved.
			ol_i := d.vars.ol_i 
			d.vars.ol_i ++
			if ol_i < d.ol_cnt {
				if d.vars.ol_i  == d.ol_cnt {
					d.vars.ol_i  = 0
					d.step = 6
				}
				D_ID := (*d.OL_D_IDs)[ol_i]
				O_ID := (*d.OL_O_IDs)[ol_i]
				OL_NUMBER := (*d.OL_NUMBERs)[ol_i]
				return tpcc_l.Read("OL_W_ID" + str(d.W_ID) + "OL_D_ID" + str(D_ID) + "OL_O_ID" + str(O_ID) + 
				"OL_NUMBER" + str(OL_NUMBER))
			}
		
		case 6:
			// The row in the CUSTOMER table with matching C_W_ID (equals W_ID), C_D_ID (equals D_ID), and C_ID (equals O_C_ID) is selected and C_BALANCE is increased by the sum of all order-line amounts (OL_AMOUNT) previously retrieved. C_DELIVERY_CNT is incremented by 1.
			dno_i := d.vars.dno_i 
			d.vars.dno_i ++
			if dno_i < d.d_cnt {
				if d.vars.dno_i == d.d_cnt {
					d.vars.dno_i = 0
					d.step = 7
				}
				D_ID := (*d.Del_NO_D_IDs)[dno_i]
				C_ID := (*d.Del_NO_C_IDs)[dno_i]
				return tpcc_l.Write("C_W_ID" + str(d.W_ID) + "C_D_ID" + str(D_ID) + "C_ID" + str(C_ID))
			}
		case 7:
			return nil
	}

	return nil
}
