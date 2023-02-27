package gria



type Version struct {
	owner_txn *TXN
	max_read_txn *TXN
}


func NewVersion(txn *TXN) *Version {
	return &(Version{txn, nil})
}	