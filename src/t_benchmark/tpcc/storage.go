package tpcc

import (
	"sync"
)

/*
TPCC:
Context and Storage
*/


type TPCC struct {

	WarehouseTable *(sync.Map) // *map[WarehousePrimaryKey](*Warehouse)
	N_warehouse int

	DistrictTable *(sync.Map) // *map[DistrictPrimaryKey](*District) 
	N_district int

	CustomerTable *(sync.Map) //*map[CustomerPrimaryKey](*Customer)

	HistoryTable *(sync.Map) // *map[HistoryPrimaryKey](*History)

	NewOrderTable *(sync.Map) // *map[NewOrderPrimaryKey](*NewOrder)

	OrderTable *(sync.Map) // *map[OrderPrimaryKey](*Order)

	OrderLineTable *(sync.Map) // *map[OrderLinePrimaryKey](*OrderLine)

	ItemTable *(sync.Map) // *map[ItemPrimaryKey](*Item)

	StockTable *(sync.Map) // *map[StockPrimaryKey](*Stock)

	
	newOrderCrossPartitionProbability int // out of 100
	paymentCrossPartitionProbability int
	payment_look_up bool
	write_to_w_ytd bool
	n_rate float64
}


func NewTPCC(warehouse int, n_rate float64) *TPCC {

	WarehouseTable := sync.Map{}

	DistrictTable := sync.Map{}

	CustomerTable := sync.Map{}

	HistoryTable := sync.Map{}

	NewOrderTable := sync.Map{}

	OrderTable := sync.Map{}

	OrderLineTable := sync.Map{}

	ItemTable := sync.Map{}

	StockTable := sync.Map{}

	// Init warehouse
	for wid :=0 ; wid < warehouse; wid ++ {
		n_w_v := Warehouse{}
		n_w_k := WarehousePrimaryKey{wid}
		WarehouseTable.Store(n_w_k, &n_w_v)
	}

	// Init district
	n_district := 0
	for wid :=0; wid < warehouse; wid ++ {
		for did :=0; did < 10; did ++ {
			n_d_v := District{}
			n_d_k := DistrictPrimaryKey{did, wid}
			DistrictTable.Store(n_d_k, n_d_v)
			n_district ++
		}
		
	}



	// Init 

	return &TPCC{&WarehouseTable, warehouse, &DistrictTable, n_district, &CustomerTable, &HistoryTable, &NewOrderTable, &OrderTable, &OrderLineTable, &ItemTable, &StockTable, 10, 15, false, true, n_rate}
}