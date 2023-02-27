package tpcc


import "fmt"
/*
schema and storage
*/
type Warehouse struct {
	
}

type WarehousePrimaryKey struct {
	W_ID int
}

func (wp *WarehousePrimaryKey) ToString() string {
	str := fmt.Sprintf("W_%d", wp.W_ID)
	return str
}

// var WarehouseTable = make(map[WarehousePrimaryKey](*Warehouse))


/*=============================== 1 =================================*/

type District struct {

}


type DistrictPrimaryKey struct {
	D_ID int
	D_W_ID int

	// D_W_ID Foreign Key, references W_ID
}

func (dp *DistrictPrimaryKey) ToString() string {
	str := fmt.Sprintf("D_%d_%d", dp.D_ID, dp.D_W_ID)
	return str
}

// var DistrictTable = make(map[DistrictPrimaryKey](*District))

/*=============================== 2 =================================*/

type Customer struct {

}


type CustomerPrimaryKey struct {
	
	C_ID int
	C_D_ID int
	C_W_ID int

	// Primary Key: (C_W_ID, C_D_ID, C_ID)
	// (C_W_ID, C_D_ID) Foreign Key, references (D_W_ID, D_ID)

}

func (cp *CustomerPrimaryKey) ToString() string {
	str := fmt.Sprintf("C_%d_%d_%d", cp.C_ID, cp.C_D_ID, cp.C_W_ID)
	return str
}

type CustomerNamePrimaryKey struct {
	
	C_LAST string
	C_D_ID int
	C_W_ID int

	// Primary Key: (C_W_ID, C_D_ID, C_ID)
	// (C_W_ID, C_D_ID) Foreign Key, references (D_W_ID, D_ID)

}

func (cnp *CustomerNamePrimaryKey) ToString() string {
	str := fmt.Sprintf("C_%s_%d_%d", cnp.C_LAST, cnp.C_D_ID, cnp.C_W_ID)
	return str
}

// var CustomerTable = make(map[CustomerPrimaryKey](*Customer))

/*=============================== 3 =================================*/

type History struct {


	H_C_ID int
	H_C_D_ID int
	H_C_W_ID int

	H_D_ID int
	H_W_ID int

	// Primary Key: none
	// (H_C_W_ID, H_C_D_ID, H_C_ID) Foreign Key, references (C_W_ID, C_D_ID, C_ID)
	// (H_W_ID, H_D_ID) Foreign Key, references (D_W_ID, D_ID)

}

type HistoryPrimaryKey struct {
	H_ID int // add a primary key for make a map
}

func (hp *HistoryPrimaryKey) ToString() string {
	str := fmt.Sprintf("H_%d", hp.H_ID)
	return str
}

// var HistoryTable = make(map[HistoryPrimaryKey](*History))



/*=============================== 4 =================================*/

type NewOrder struct {

}


type NewOrderPrimaryKey struct {

	NO_O_ID int
	NO_D_ID int
	NO_W_ID int

	// Primary Key: (NO_W_ID, NO_D_ID, NO_O_ID)
	// (NO_W_ID, NO_D_ID, NO_O_ID) Foreign Key, references (O_W_ID, O_D_ID, O_ID)

}

func (nop *NewOrderPrimaryKey) ToString() string {
	str := fmt.Sprintf("NO_%d_%d_%d", nop.NO_O_ID, nop.NO_D_ID, nop.NO_W_ID)
	return str
}

// var NewOrderTable = make(map[NewOrderPrimaryKey](*NewOrder))

/*=============================== 5 =================================*/

type Order struct {
	
	O_C_ID int

	// (O_W_ID, O_D_ID, O_C_ID) Foreign Key, references (C_W_ID, C_D_ID, C_ID)
	
}

type OrderPrimaryKey struct {
	O_W_ID int
	O_D_ID int
	O_ID int
}

func (op *OrderPrimaryKey) ToString() string {
	str := fmt.Sprintf("O_%d_%d_%d", op.O_W_ID, op.O_D_ID, op.O_ID)
	return str
}

// var OrderTable = make(map[OrderPrimaryKey](*Order))

/*=============================== 6 =================================*/

type OrderLine struct {
	

	OL_I_ID int
	OL_SUPPLY_W_ID int

	// (OL_W_ID, OL_D_ID, OL_O_ID) Foreign Key, references (O_W_ID, O_D_ID, O_ID)
	// (OL_SUPPLY_W_ID, OL_I_ID) Foreign Key, references (S_W_ID, S_I_ID)

}

type OrderLinePrimaryKey struct {
	OL_W_ID int
	OL_D_ID int
	OL_O_ID int
	OL_NUMBER int
}

func (olp *OrderLinePrimaryKey) ToString() string {
	str := fmt.Sprintf("OL_%d_%d_%d_%d", olp.OL_W_ID, olp.OL_D_ID, olp.OL_O_ID, olp.OL_NUMBER)
	return str
}

// var OrderLineTable = make(map[OrderLinePrimaryKey](*OrderLine))

/*=============================== 7 =================================*/

type Item struct {
	
	I_IM_ID int
}


type ItemPrimaryKey struct {
	I_ID int
}

func (ip *ItemPrimaryKey) ToString() string {
	str := fmt.Sprintf("I_%d", ip.I_ID)
	return str
}

// var ItemTable = make(map[ItemPrimaryKey](*Item))

/*=============================== 8 =================================*/

type Stock struct {
	
	// S_W_ID Foreign Key, references W_ID
	// S_I_ID Foreign Key, references I_ID

}


type StockPrimaryKey struct {
	S_I_ID int
	S_W_ID int
}

func (sp *StockPrimaryKey) ToString() string {
	str := fmt.Sprintf("S_%d_%d", sp.S_I_ID, sp.S_W_ID)
	return str
}

// var StockTable = make(map[StockPrimaryKey](*Stock))

/*=============================== 9 =================================*/

