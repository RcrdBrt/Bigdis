package storage

import (
	"bigdis/utils"
	"bytes"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

func Get(dbNum int, args [][]byte, dbOp *dbOperation) ([]byte, error) {
	var err error
	dbOp, err = startDBOperation(dbOp, false)
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
	dbOp, err = startDBOperation(dbOp, true)
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
	dbOp, err := startDBOperation(nil, true)
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

func Incr(dbNum int, args [][]byte) (int, error) {
	dbOp, err := startDBOperation(nil, true)
	if err != nil {
		return 0, err
	}
	dbOp.chainDBOperation()

	newValue, err := IncrBy(dbNum, [][]byte{args[0], []byte("1")}, dbOp)
	if err != nil {
		return 0, err
	}

	dbOp.unchainDBOperation()
	if err := dbOp.endDBOperation(); err != nil {
		return 0, err
	}

	return newValue, nil
}

func IncrBy(dbNum int, args [][]byte, dbOp *dbOperation) (int, error) {
	var err error
	dbOp, err = startDBOperation(dbOp, true)
	if err != nil {
		return 0, err
	}
	wasChained := dbOp.chainDBOperation()

	// check if user input is an integer
	userIncr, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return 0, utils.ErrNotInteger
	}

	value, err := Get(dbNum, args, dbOp)
	if err != nil {
		return 0, err
	}

	var newValue int
	if value == nil {
		newValue = userIncr
	} else {
		newValue, err = strconv.Atoi(string(value))
		if err != nil {
			return 0, utils.ErrNotInteger
		}
		newValue += userIncr
	}

	args[1] = []byte(strconv.Itoa(newValue))

	if err := Set(dbNum, args, dbOp); err != nil {
		return 0, err
	}

	if !wasChained {
		dbOp.unchainDBOperation()
	}
	if err := dbOp.endDBOperation(); err != nil {
		return 0, err
	}

	return newValue, nil
}

func GetSet(dbNum int, args [][]byte) ([]byte, error) {
	dbOp, err := startDBOperation(nil, true)
	if err != nil {
		return nil, err
	}
	dbOp.chainDBOperation()

	value, err := Get(dbNum, args, dbOp)
	if err != nil {
		return nil, err
	}

	if err := Set(dbNum, args, dbOp); err != nil {
		return nil, err
	}

	dbOp.unchainDBOperation()
	if err := dbOp.endDBOperation(); err != nil {
		return nil, err
	}

	return value, nil
}

func Strlen(dbNum int, args [][]byte) (int, error) {
	dbOp, err := startDBOperation(nil, false)
	if err != nil {
		return 0, err
	}

	var length int
	if err := dbOp.Txn.QueryRow(fmt.Sprintf("SELECT length(value) FROM bigdis_%d WHERE key = ? and type='s'", dbNum), args[0]).Scan(&length); err != nil {
		if err == sql.ErrNoRows {
			// check if key exists of type other than string
			var exists bool
			if err := dbOp.Txn.QueryRow(fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM bigdis_%d WHERE key = ?)", dbNum), args[0]).Scan(&exists); err != nil {
				return 0, err
			}

			if exists {
				return 0, utils.ErrWrongType
			}

			return 0, nil
		}

		return 0, err
	}

	if err := dbOp.endDBOperation(); err != nil {
		return 0, err
	}

	return length, nil
}

func Append(dbNum int, args [][]byte) (int, error) {
	dbOp, err := startDBOperation(nil, true)
	if err != nil {
		return 0, err
	}
	dbOp.chainDBOperation()

	value, err := Get(dbNum, args, dbOp)
	if err != nil {
		return 0, err
	}

	var newValue []byte
	if value == nil {
		newValue = args[1]
	} else {
		newValue = append(value, args[1]...)
	}

	args[1] = newValue

	if err := Set(dbNum, args, dbOp); err != nil {
		return 0, err
	}

	dbOp.unchainDBOperation()
	if err := dbOp.endDBOperation(); err != nil {
		return 0, err
	}

	return len(newValue), nil
}

func Decr(dbNum int, args [][]byte) (int, error) {
	dbOp, err := startDBOperation(nil, true)
	if err != nil {
		return 0, err
	}
	dbOp.chainDBOperation()

	args[1] = []byte("-1")

	newValue, err := IncrBy(dbNum, args, dbOp)
	if err != nil {
		return 0, err
	}

	dbOp.unchainDBOperation()
	if err := dbOp.endDBOperation(); err != nil {
		return 0, err
	}

	return newValue, nil
}

func DecrBy(dbNum int, args [][]byte) (int, error) {
	dbOp, err := startDBOperation(nil, true)
	if err != nil {
		return 0, err
	}
	dbOp.chainDBOperation()

	// check if user input is an integer
	userDecr, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return 0, utils.ErrNotInteger
	}

	// reverse user input
	args[1] = []byte(strconv.Itoa(userDecr * -1))

	newValue, err := IncrBy(dbNum, args, dbOp)
	if err != nil {
		return 0, err
	}

	dbOp.unchainDBOperation()
	if err := dbOp.endDBOperation(); err != nil {
		return 0, err
	}

	return newValue, nil
}

func MGet(dbNum int, args [][]byte) ([]any, error) {
	var anyArgs []any
	for i := range args {
		anyArgs = append(anyArgs, args[i])
	}

	dbOp, err := startDBOperation(nil, false)
	if err != nil {
		return nil, err
	}

	rows, err := dbOp.Txn.Query(fmt.Sprintf("SELECT key, value FROM bigdis_%d WHERE key IN (%s)", dbNum, strings.Repeat("?,", len(args)-1)+"?"), anyArgs...)
	if err != nil {
		return nil, err
	}

	var values []any
	var returnedKeys [][]byte // needed to fill in nil values for keys not found
	for rows.Next() {
		var key, value []byte
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}

		returnedKeys = append(returnedKeys, key)
		values = append(values, value)
	}

	if err := rows.Close(); err != nil {
		return nil, err
	}

	// must append (in the right spot) nil if key is not found
	for i := range args {
		if i >= len(returnedKeys) || !bytes.Equal(args[i], returnedKeys[i]) {
			values = append(values[:i+1], values[i:]...)
			values[i] = nil

			// must also append to returnedKeys
			returnedKeys = append(returnedKeys[:i+1], returnedKeys[i:]...)
			returnedKeys[i] = args[i]
		}
	}

	if err := dbOp.endDBOperation(); err != nil {
		return nil, err
	}

	return values, nil
}

func MSet(dbNum int, args [][]byte, dbOp *dbOperation) error {
	var anyArgs []any
	for i := range args {
		anyArgs = append(anyArgs, args[i])
	}

	var err error
	dbOp, err = startDBOperation(dbOp, true)
	if err != nil {
		return err
	}

	// insert all keys and values in one shot

	if _, err := dbOp.Txn.Exec(fmt.Sprintf(`
		INSERT INTO bigdis_%d (key, value, type) VALUES %s
		ON CONFLICT(key) DO UPDATE SET
			value = excluded.value,
			updated = current_timestamp`, dbNum, strings.Repeat("(?, ?, 's'),", len(args)/2-1)+"(?, ?, 's')"), anyArgs...); err != nil {
		return err
	}

	if err := dbOp.endDBOperation(); err != nil {
		return err
	}

	return nil
}

func MSetNX(dbNum int, args [][]byte) (int, error) {
	var keys [][]byte
	for i := 0; i < len(args); i += 2 {
		keys = append(keys, args[i])
	}

	dbOp, err := startDBOperation(nil, true)
	if err != nil {
		return 0, err
	}
	dbOp.chainDBOperation()

	// check if any of the keys exist
	count, err := Exists(dbNum, keys, dbOp)
	if err != nil {
		return 0, err
	}

	if count > 0 {
		return 0, nil
	}

	if err := MSet(dbNum, args, dbOp); err != nil {
		return 0, err
	}

	dbOp.unchainDBOperation()
	if err := dbOp.endDBOperation(); err != nil {
		return 0, err
	}

	return 1, nil
}

func SetNX(dbNum int, args [][]byte) (int, error) {
	result, err := MSetNX(dbNum, args)
	if err != nil {
		return 0, err
	}

	return result, nil
}
