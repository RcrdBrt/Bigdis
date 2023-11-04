package storage

import "database/sql"

type dbOperation struct {
	Txn     *sql.Tx
	ChainOp bool
}

func startDBOperation(dbOp *dbOperation) (*dbOperation, error) {
	if dbOp == nil {
		txn, err := DB.Begin()
		if err != nil {
			return nil, err
		}
		dbOp = &dbOperation{
			ChainOp: false,
			Txn:     txn,
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

func (dbOp *dbOperation) chainDBOperation() {
	dbOp.ChainOp = true
}

func (dbOp *dbOperation) unchainDBOperation() {
	dbOp.ChainOp = false
}
