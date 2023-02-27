package tpcc


import (
	"t_util"
	"t_benchmark/tpcc/math"
	"sync"
	// "fmt"
	"t_txn"
	// "t_log"
)

const (
	NEW_ORDER int = 1
	PAYMENT = 2
	
)



// type OPS interface {
// 	Next() bool
// 	Get() *t_txn.OP
// 	Reset()
// }



type NewOrderOPS struct {
	tpcc *TPCC
	state_idx int
	cur_key *t_txn.OP

	W_ID int
	D_ID int
	C_ID int
	O_OL_CNT int

	INFO []NewOrderQueryInfo

	LocalStore *(sync.Map)

	i_1 int
	i_2 int
}


func (query *NewOrderOPS) DataGeneration() {

	query.i_1 = 0
	query.i_2 = 0

	context := query.tpcc
	// For any given terminal, the home warehouse number (W_ID) is constant over the whole measurement interval
	W_ID := t_util.RandInt(context.N_warehouse)
	// fmt.Println(context.N_warehouse)
	query.W_ID = W_ID;
	// The district number (D_ID) is randomly selected within [1 .. 10]
	// from the home warehouse (D_W_ID = W_ID).
	query.D_ID = t_util.RandInt(context.N_district);
	// The non-uniform random customer number (C_ID) is selected using the
	// NURand(1023,1,3000) function from the selected district number (C_D_ID =
	// D_ID) and the home warehouse number (C_W_ID = W_ID).
	query.C_ID = math.NURand(1023, 1, 3000);
	
	// The number of items in the order (ol_cnt) is randomly selected within [5 ... 15] (an average of 10).
	query.O_OL_CNT = t_util.RandIntFromTo(5, 15);
	rbk := t_util.RandIntFromTo(1, 100);
	var i int
	for i = 0; i < query.O_OL_CNT; i++ {
		// A non-uniform random item number (OL_I_ID) is selected using the
		// NURand(8191,1,100000) function. If this is the last item on the order
		// and rbk = 1 (see Clause 2.4.1.4), then the item number is set to an
		// unused value.
	
		retry := true
		for retry {
		  retry = false;
		  query.INFO[i].OL_I_ID = math.NURand(8191, 1, 100000);
		  for k := 0; k < i; k++ {
			if query.INFO[k].OL_I_ID == query.INFO[i].OL_I_ID { // the 15 OL_I_ID should be different
			  retry = true;
			  break
			}
		  }
		}
	
		if i == query.O_OL_CNT - 1 && rbk == 1 {
		  query.INFO[i].OL_I_ID = 0;
		}
	
		// The first supplying warehouse number (OL_SUPPLY_W_ID) is selected as
		  // the home warehouse 90% of the time and as a remote warehouse 10% of the
		  // time.
		  if (i == 0) {
			x := t_util.RandIntFromTo(1, 100);
			if (x <= context.newOrderCrossPartitionProbability && context.N_warehouse > 1) { // 10% cross warehouse
				  OL_SUPPLY_W_ID := W_ID;
				  for OL_SUPPLY_W_ID == W_ID {
					OL_SUPPLY_W_ID = t_util.RandIntFromTo(0, context.N_warehouse); // id begin with 0
				  }
				  query.INFO[i].OL_SUPPLY_W_ID = OL_SUPPLY_W_ID;
			} else {
				  query.INFO[i].OL_SUPPLY_W_ID = W_ID;
			}
		  } else {
			query.INFO[i].OL_SUPPLY_W_ID = W_ID;
		  }
		  query.INFO[i].OL_QUANTITY = t_util.RandIntFromTo(1, 10);
	}
	// Profile

	// The input data (see Clause 2.4.3.2) are communicated to the SUT.
	// The row in the WAREHOUSE table with matching W_ID is selected and W_TAX, the warehouse tax rate, is retrieved.
	query.cur_key = &(t_txn.OP{(&WarehousePrimaryKey{query.W_ID}).ToString(), false})
	query.state_idx = 1
}


type PaymentOPS struct {
	tpcc *TPCC
	state_idx int
	cur_key *t_txn.OP


	W_ID int
  	D_ID int
  	C_ID int
  	C_LAST string
  	C_D_ID int
  	C_W_ID int
  	H_AMOUNT float64
	
}

func (query *PaymentOPS) DataGeneration() {
	context := query.tpcc
		
	// For any given terminal, the home warehouse number (W_ID) is constant over the whole measurement interval
	W_ID := t_util.RandInt(context.N_warehouse)
	// fmt.Println(context.N_warehouse)

	query.W_ID = W_ID;
	// The district number (D_ID) is randomly selected within [1 .. 10]
	// from the home warehouse (D_W_ID = W_ID).
	query.D_ID = t_util.RandInt(context.N_district);

	// the customer resident warehouse is the home warehouse 85% of the time
	// and is a randomly selected remote warehouse 15% of the time.
	// If the system is configured for a single warehouse,
	// then all customers are selected from that single home warehouse.
	x := t_util.RandIntFromTo(1, 100)
	if x <= context.newOrderCrossPartitionProbability && context.N_warehouse > 1 {
		// If x <= 15 a customer is selected from a random district number (C_D_ID
		  // is randomly selected within [1 .. context.n_district]), and a random
		  // remote warehouse number (C_W_ID is randomly selected within the range
		  // of active warehouses (see Clause 4.2.2), and C_W_ID ≠ W_ID).
		C_W_ID := W_ID
		for (C_W_ID == W_ID) {
			C_W_ID = t_util.RandIntFromTo(0, context.N_warehouse)
		}
		query.C_W_ID = C_W_ID;
		query.C_D_ID = t_util.RandIntFromTo(0, context.N_district)
	} else {
		// If x > 15 a customer is selected from the selected district number
		  // (C_D_ID = D_ID) and the home warehouse number (C_W_ID = W_ID).
		query.C_D_ID = query.D_ID;
		query.C_W_ID = W_ID;
	}
	// a CID is always used.
	y := t_util.RandIntFromTo(1, 100)
	// The customer is randomly selected 60% of the time by last name (C_W_ID ,
	// C_D_ID, C_LAST) and 40% of the time by number (C_W_ID , C_D_ID , C_ID).
	if (y <= 60 && context.payment_look_up) {
		// If y <= 60 a customer last name (C_LAST) is generated according to
		  // Clause 4.3.2.3 from a non-uniform random value using the
		  // NURand(255,0,999) function.
		
		query.C_LAST = math.RandLastName(math.NURand(255, 0, 999))
		query.C_ID = 0
	} else {
		// If y > 60 a non-uniform random customer number (C_ID) is selected using
		  // the NURand(1023,1,3000) function.
		query.C_ID = math.NURand(1023, 1, 3000)
	}
	// The payment amount (H_AMOUNT) is randomly selected within [1.00 ..
	// 5,000.00].
	query.H_AMOUNT = float64(t_util.RandIntFromTo(1, 5000))
	
	// The row in the WAREHOUSE table with matching W_ID is selected.
	  // W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, and W_ZIP are
	  // retrieved and W_YTD,
	
	query.cur_key = &(t_txn.OP{(&WarehousePrimaryKey{query.W_ID}).ToString(), true})
	query.state_idx = 1

}


func (tpcc *TPCC) NewOPS() t_txn.AccessPtr {
	if t_util.RandFloat() < tpcc.n_rate {
		local_s := sync.Map{}
		info := make([]NewOrderQueryInfo, 15)
		query := &NewOrderOPS{tpcc, 0, nil, -1, -1, -1, -1, info, &local_s, 0, 0}
		query.DataGeneration()
		return query
	} else {
		// fmt.Println("into new ops")
		query := &PaymentOPS{tpcc, 0, nil, 0, 0, 0, "", 0, 0, 0.0}
		query.DataGeneration()
		// fmt.Println("end new ops")
		return query
	}
}

type NewOrderQueryInfo struct {
    OL_I_ID int
    OL_SUPPLY_W_ID int
    OL_QUANTITY int
}

/* useless method */
func (query *NewOrderOPS) Len() int {
	return -1
}



func (query *NewOrderOPS) ReadWriteSeq() *([](*t_txn.OP)) {
	return nil
}


func (query *NewOrderOPS) ReadWriteMap(is_write bool) *map[string]int {
	return nil
}

func (query *PaymentOPS) Len() int {
	return -1
}



func (query *PaymentOPS) ReadWriteSeq() *([](*t_txn.OP)) {
	return nil
}


func (query *PaymentOPS) ReadWriteMap(is_write bool) *map[string]int {
	return nil
}

/*
logic always true, even the last one(but Get Nil)
Next change the state_idx and cur_key, next_idx
*/
func (query *NewOrderOPS) Next() bool {

	switch query.state_idx {
	case 1:
		// The row in the DISTRICT table with matching D_W_ID and D_ ID is selected,
    	// D_TAX, the district tax rate, is retrieved, and D_NEXT_O_ID, the next
    	// available order number for the district, is retrieved and incremented by
   		// one.
		
		query.cur_key = &(t_txn.OP{(&DistrictPrimaryKey{query.D_ID, query.W_ID}).ToString(), true}) // for update
		query.state_idx = 2
		return true
	case 2:
		query.cur_key = &(t_txn.OP{(&CustomerPrimaryKey{query.C_ID, query.D_ID, query.W_ID}).ToString(), false})
		query.state_idx = 3
		return true
	case 3:
		// The row in the ITEM table with matching I_ID (equals OL_I_ID) is
		// selected and I_PRICE, the price of the item, I_NAME, the name of the
		// item, and I_DATA are retrieved. If I_ID has an unused value (see
		// Clause 2.4.1.5), a "not-found" condition is signaled, resulting in a
		// rollback of the database transaction (see Clause 2.4.2.3).
		/* index out of range with length 15*/

		// t_log.Log(t_log.INFO, "query:%p, %d can not >= %d\n", query, query.i_1, query.O_OL_CNT)
		query.cur_key = &(t_txn.OP{(&ItemPrimaryKey{query.INFO[query.i_1].OL_I_ID}).ToString(), false})
		query.state_idx = 4
		return true

	case 4:
		// The row in the STOCK table with matching S_I_ID (equals OL_I_ID) and
		// S_W_ID (equals OL_SUPPLY_W_ID) is selected.
		query.cur_key = &(t_txn.OP{(&StockPrimaryKey{query.INFO[query.i_1].OL_I_ID, query.INFO[query.i_1].OL_SUPPLY_W_ID}).ToString(), false})
		query.i_1 ++
		if query.i_1  < query.O_OL_CNT {
			query.state_idx = 3
			return true
		} else {
			query.state_idx = 5
			return true
		}

	case 5:
		query.cur_key = &(t_txn.OP{(&DistrictPrimaryKey{query.D_ID, query.W_ID}).ToString(), true}) // for update
		query.state_idx = 6
	case 6:
		query.cur_key = &(t_txn.OP{(&StockPrimaryKey{query.INFO[query.i_2].OL_I_ID, query.INFO[query.i_2].OL_SUPPLY_W_ID}).ToString(), true}) // for update
		query.i_2 ++
		if query.i_2 < query.O_OL_CNT {
			query.state_idx = 6
			return true
		} else {
			query.state_idx = 0
		}
		// int i = 0; i < query.O_OL_CNT; i++
	case 0:
		query.cur_key = nil
		return true
	}
	return true
}

func (not *NewOrderOPS) Get() *t_txn.OP {
	return not.cur_key
}


func (not *NewOrderOPS) Reset() {
	not.DataGeneration()
	not.state_idx = 1
}




func (query *PaymentOPS) Next() bool {
	// fmt.Println(query.state_idx)
	context := query.tpcc
	switch query.state_idx {
		case 1:
			// The row in the DISTRICT table with matching D_W_ID and D_ID is selected.
    		// D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, and D_ZIP are retrieved
    		// and D_YTD,
			query.cur_key = &(t_txn.OP{(&DistrictPrimaryKey{query.D_ID, query.W_ID}).ToString(), true}) // for update
			if query.C_ID == 0 {
				query.state_idx = 2
			} else {
				query.state_idx = 3
			}
			return true

		case 2:
			// The row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is
    		// selected and C_DISCOUNT, the customer's discount rate, C_LAST, the
    		// customer's last name, and C_CREDIT, the customer's credit status, are
    		// retrieved.
			
			query.cur_key = &(t_txn.OP{(&CustomerNamePrimaryKey{query.C_LAST, query.C_D_ID, query.C_W_ID}).ToString(), false})
			query.state_idx = 3
			return true
		case 3:
			query.cur_key = &(t_txn.OP{(&CustomerPrimaryKey{query.C_ID, query.C_D_ID, query.C_W_ID}).ToString(), true})
			if context.write_to_w_ytd {
				query.state_idx = 4
			} else {
				query.state_idx = 5
			}
			return true
		
		case 4:
			// the warehouse's year-to-date balance, is increased by H_ AMOUNT.

			query.cur_key = &(t_txn.OP{(&WarehousePrimaryKey{query.W_ID}).ToString(), true})
			query.state_idx = 5
			return true
		case 5:
			// the district's year-to-date balance, is increased by H_AMOUNT.

			query.cur_key = &(t_txn.OP{(&DistrictPrimaryKey{query.D_ID, query.W_ID}).ToString(), true})
			query.state_idx = 6
			return true
		
		case 6:
			query.cur_key = &(t_txn.OP{(&CustomerPrimaryKey{query.C_ID, query.C_D_ID, query.C_W_ID}).ToString(), true})
			query.state_idx = 0
			return true
		
		case 0:
			query.cur_key = nil
			return true

	}
	return true
}


func (not *PaymentOPS) Get() *t_txn.OP {
	return not.cur_key
}


func (not *PaymentOPS) Reset() {
	not.DataGeneration()
	not.state_idx = 1
}