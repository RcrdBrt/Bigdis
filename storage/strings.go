package storage

import (
	"bigdis/utils"
	"database/sql"
	"fmt"
	"strings"
)

func Get(dbNum int, args [][]byte, dbOp *dbOperation) ([]byte, error) {
	var err error
	dbOp, err = startDBOperation(dbOp)
	if err != nil {
		return nil, err
	}

	var value []byte
	if err := dbOp.Txn.QueryRow(fmt.Sprintf("SELECT value FROM bigdis_%d WHERE key = ?", dbNum), args[0]).Scan(&value); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}

		return nil, err
	}

	if err := dbOp.endDBOperation(); err != nil {
		return nil, err
	}

	return value, nil
}

func Set(dbNum int, args [][]byte, dbOp *dbOperation) error {
	var err error
	dbOp, err = startDBOperation(dbOp)
	if err != nil {
		return err
	}

	switch len(args) {
	case 0:
		fallthrough
	case 1:
		return utils.ErrWrongSyntax
	case 2:
		_, err := dbOp.Txn.Exec(fmt.Sprintf(`
		INSERT INTO bigdis_%d (key, value, type) VALUES (?, ?, 's')
		ON CONFLICT(key) DO UPDATE SET
			value = ?,
			updated = current_timestamp`, dbNum), args[0], args[1], args[1])
		if err != nil {
			return err
		}
	case 3:
		count, err := Exists(dbNum, [][]byte{args[0]}, dbOp)
		if err != nil {
			return err
		}

		if strings.ToLower(string((args[2]))) == "nx" {
			if count > 0 {
				return nil
			}
			_, err := dbOp.Txn.Exec(fmt.Sprintf(`
				INSERT INTO bigdis_%d (key, value, type) VALUES (?, ?, 's')
			`, dbNum), args[0], args[1])
			if err != nil {
				return err
			}
		} else if strings.ToLower(string((args[2]))) == "xx" {
			if count == 0 {
				return nil
			}
			_, err := dbOp.Txn.Exec(fmt.Sprintf(`
				update bigdis_%d set value = ?, updated = current_timestamp where key = ?
			`, dbNum), args[1], args[0])
			if err != nil {
				return err
			}
		} else {
			return utils.ErrWrongSyntax
		}
	}

	if err := dbOp.endDBOperation(); err != nil {
		return err
	}

	return nil
}

func GetDel(dbNum int, args [][]byte) ([]byte, error) {
	dbOp, err := startDBOperation(nil)
	if err != nil {
		return nil, err
	}
	dbOp.chainDBOperation()

	value, err := Get(dbNum, args, dbOp)
	if err != nil {
		return nil, err
	}

	// cannot chain the deletion
	// must check for type string in the db as per redis spec
	if _, err := dbOp.Txn.Exec(fmt.Sprintf("DELETE FROM bigdis_%d WHERE key = ? and type = 's'", dbNum), args[0]); err != nil {
		return nil, err
	}

	dbOp.unchainDBOperation()
	if err := dbOp.endDBOperation(); err != nil {
		return nil, err
	}

	return value, nil
}
