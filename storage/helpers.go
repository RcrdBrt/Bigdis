package storage

import (
	"database/sql"
	"errors"
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

func (dbOp *dbOperation) dummyWrite() error {
	if dbOp.Txn == nil {
		return errors.New("dbOp.Txn is nil")
	}

	if !dbOp.WritePool {
		return errors.New("dbOp.WritePool is false")
	}

	if _, err := dbOp.Txn.Exec("update redis_type set type = type where type = 's'"); err != nil {
		return err
	}

	return nil
}

func insert[T any](a []T, index int, value T) []T {
	n := len(a)
	if index < 0 {
		index = (index%n + n) % n
	}
	switch {
	case index == n: // nil or empty slice or after last element
		return append(a, value)

	case index < n: // index < len(a)
		a = append(a[:index+1], a[index:]...)
		a[index] = value
		return a

	case index < cap(a): // index > len(a)
		a = a[:index+1]
		var zero T
		for i := n; i < index; i++ {
			a[i] = zero
		}
		a[index] = value
		return a

	default:
		b := make([]T, index+1) // malloc
		if n > 0 {
			copy(b, a)
		}
		b[index] = value
		return b
	}
}
