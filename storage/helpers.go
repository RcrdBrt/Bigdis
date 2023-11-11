package storage

import (
	"database/sql"
)

type dbOperation struct {
	Txn       *sql.Tx
	ChainOp   bool
	WritePool bool
}

func startDBOperation(dbOp *dbOperation, writePool bool) (*dbOperation, error) {
	if dbOp == nil {
		if writePool {
			txn, err := DBwp.Begin()
			if err != nil {
				return nil, err
			}
			dbOp = &dbOperation{
				ChainOp:   false,
				Txn:       txn,
				WritePool: true,
			}
		} else {
			txn, err := DBrp.Begin()
			if err != nil {
				return nil, err
			}
			dbOp = &dbOperation{
				ChainOp:   false,
				Txn:       txn,
				WritePool: false,
			}
		}
	}

	return dbOp, nil
}

func (dbOp *dbOperation) endDBOperation() error {
	if !dbOp.ChainOp {
		defer dbOp.Txn.Rollback()
		if err := dbOp.Txn.Commit(); err != nil {
			return err
		}
	}

	return nil
}

// returns the original bool value of ChainOp
func (dbOp *dbOperation) chainDBOperation() bool {
	oldChainOp := dbOp.ChainOp
	dbOp.ChainOp = true
	return oldChainOp
}

func (dbOp *dbOperation) unchainDBOperation() {
	dbOp.ChainOp = false
}
