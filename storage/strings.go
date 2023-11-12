package storage

import (
	"bigdis/utils"
	"bytes"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func Get(dbNum int, args [][]byte, dbOp *dbOperation) ([]byte, error) {
	var err error
	dbOp, err = startDBOperation(dbOp, false)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := dbOp.endDBOperation(); err != nil {
			utils.Print("Error while ending DB operation: %s\n", err)
		}
	}()

	var value []byte
	var exp sql.NullTime
	var keyType string
	if err := dbOp.Txn.QueryRow(fmt.Sprintf("SELECT value, exp, type FROM bigdis_%d WHERE key = ?", dbNum), args[0]).Scan(&value, &exp, &keyType); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}

		return nil, err
	}

	if keyType != "s" {
		return nil, utils.ErrWrongType
	}

	if exp.Valid && exp.Time.Before(time.Now().UTC()) {
		return nil, nil
	}

	return value, nil
}

/*
set returns bulkReplyValue in form of []byte and error.
In case of error, bulkReplyValue is nil.

set could write a statusreply or a bulk reply (get parameter of the set command).
*/
func Set(dbNum int, args [][]byte, dbOp *dbOperation) ([]byte, error) {
	var replyBytes []byte
	var err error
	dbOp, err = startDBOperation(dbOp, true)
	if err != nil {
		return replyBytes, err
	}
	wasChained := dbOp.chainDBOperation()
	defer func() {
		if !wasChained {
			dbOp.unchainDBOperation()
		}
		if err := dbOp.endDBOperation(); err != nil {
			utils.Print("Error while ending DB operation: %s\n", err)
		}
	}()

	if len(args) == 2 {
		if _, err := dbOp.Txn.Exec(fmt.Sprintf(`
			INSERT INTO bigdis_%d (key, value, type) VALUES (?, ?, 's')
			ON CONFLICT(key) DO UPDATE SET
				value = ?,
				updated = current_timestamp
			where key = ?`, dbNum), args[0], args[1], args[1], args[0]); err != nil {
			return replyBytes, err
		}

		return replyBytes, nil
	}

	// extended set command features, expensive operation
	count, err := Exists(dbNum, [][]byte{args[0]}, dbOp)
	if err != nil {
		return replyBytes, err
	}

	var currentArg = 1
	setGrammar := make(map[string]struct{})
	setGrammar["existence"] = struct{}{}
	setGrammar["expiration"] = struct{}{}
	setGrammar["get"] = struct{}{}
	var expirationSeconds bool

parse_args:
	currentArg++
	if currentArg >= len(args) {
		dbOp.unchainDBOperation()
		return replyBytes, nil
	}

	switch strings.ToLower(string((args[currentArg]))) {
	case "nx":
		if _, ok := setGrammar["existence"]; !ok {
			return replyBytes, utils.ErrWrongSyntax
		}
		delete(setGrammar, "existence")

		if count > 0 {
			replyBytes = []byte{}
			return replyBytes, nil
		}

		if _, err := Set(dbNum, [][]byte{args[0], args[1]}, dbOp); err != nil {
			return replyBytes, err
		}

		goto parse_args
	case "xx":
		if _, ok := setGrammar["existence"]; !ok {
			return replyBytes, utils.ErrWrongSyntax
		}
		delete(setGrammar, "existence")

		if count == 0 {
			replyBytes = []byte{}
			return replyBytes, nil
		}

		if _, err := Set(dbNum, [][]byte{args[0], args[1]}, dbOp); err != nil {
			return replyBytes, err
		}

		goto parse_args
	case "ex":
		expirationSeconds = true
		fallthrough
	case "px":
		if _, ok := setGrammar["expiration"]; !ok {
			return replyBytes, utils.ErrWrongSyntax
		}
		delete(setGrammar, "expiration")

		// ex and px need an argument
		if currentArg+1 >= len(args) {
			return replyBytes, utils.ErrWrongSyntax
		}

		// check if user input is an integer
		//
		// not using Atoi so no need to convert back and forth to int64 for later calls
		userExp, err := strconv.ParseInt(string(args[currentArg+1]), 10, 64)
		if err != nil {
			return replyBytes, utils.ErrSyntaxError
		}

		// convert to unix timestamp

		// convert to time.Time, userExp can be seconds or milliseconds
		var userExpTime time.Time
		if expirationSeconds {
			userExpTime = time.Unix(time.Now().UTC().Unix()+userExp, 0)
		} else {
			userExpTime = time.Unix(time.Now().UTC().Unix(), userExp*1000000)
		}

		if _, err := dbOp.Txn.Exec(fmt.Sprintf(`
			INSERT INTO bigdis_%d (key, value, type, exp) VALUES (?, ?, 's', ?)
			ON CONFLICT(key) DO UPDATE SET
				value = ?,
				updated = current_timestamp,
				exp = ?
			where key = ?`, dbNum), args[0], args[1], userExpTime, args[1], userExpTime, args[0]); err != nil {
			return replyBytes, err
		}

		goto parse_args
	case "keepttl":
		if _, ok := setGrammar["expiration"]; !ok {
			return replyBytes, utils.ErrWrongSyntax
		}
		delete(setGrammar, "expiration")

		// just insert, disregard the expiration
		if _, err := Set(dbNum, [][]byte{args[0], args[1]}, dbOp); err != nil {
			return replyBytes, err
		}

		goto parse_args
	case "exat":
		expirationSeconds = true
		fallthrough
	case "pxat":
		if _, ok := setGrammar["expiration"]; !ok {
			return replyBytes, utils.ErrWrongSyntax
		}
		delete(setGrammar, "expiration")

		// exat and pxat need an argument
		if currentArg+1 >= len(args) {
			return replyBytes, utils.ErrWrongSyntax
		}

		// check if user input is an integer
		//
		// not using Atoi so no need to convert back and forth to int64 for later calls
		userExp, err := strconv.ParseInt(string(args[currentArg+1]), 10, 64)
		if err != nil {
			return replyBytes, utils.ErrSyntaxError
		}

		// convert to time.Time, userExp can be seconds or milliseconds
		var userExpTime time.Time
		if expirationSeconds {
			userExpTime = time.Unix(userExp, 0)
		} else {
			userExpTime = time.Unix(0, userExp*1000000)
		}

		if _, err := dbOp.Txn.Exec(fmt.Sprintf(`
			INSERT INTO bigdis_%d (key, value, type, exp) VALUES (?, ?, 's', ?)
			ON CONFLICT(key) DO UPDATE SET
				value = ?,
				updated = current_timestamp,
				exp = ?
			where key = ?`, dbNum), args[0], args[1], userExpTime, args[1], userExpTime, args[0]); err != nil {
			return replyBytes, err
		}

		goto parse_args
	case "get":
		if _, ok := setGrammar["get"]; !ok {
			return replyBytes, utils.ErrWrongSyntax
		}
		delete(setGrammar, "get")

		value, err := GetSet(dbNum, [][]byte{args[0], args[1]}, dbOp)
		if err != nil {
			if err := dbOp.Txn.Rollback(); err != nil {
				return replyBytes, err
			}

			return replyBytes, err
		}

		if len(value) > 0 {
			replyBytes = value
		} else {
			// Return bulk reply with nil to the client.
			// Non-nil empty slice is to signal to the handler
			// that a bulk reply is needed
			replyBytes = []byte{}
		}

		goto parse_args
	default:
		return replyBytes, utils.ErrWrongSyntax
	}

	return replyBytes, nil
}

func GetDel(dbNum int, args [][]byte) ([]byte, error) {
	dbOp, err := startDBOperation(nil, true)
	if err != nil {
		return nil, err
	}
	dbOp.chainDBOperation()
	defer func() {
		dbOp.unchainDBOperation()
		if err := dbOp.endDBOperation(); err != nil {
			utils.Print("Error while ending DB operation: %s\n", err)
		}
	}()

	value, err := Get(dbNum, args, dbOp)
	if err != nil {
		return nil, err
	}

	// cannot chain the deletion
	// must check for type string in the db as per redis spec
	if _, err := dbOp.Txn.Exec(fmt.Sprintf("DELETE FROM bigdis_%d WHERE key = ? and type = 's'", dbNum), args[0]); err != nil {
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
	defer func() {
		dbOp.unchainDBOperation()
		if err := dbOp.endDBOperation(); err != nil {
			utils.Print("Error while ending DB operation: %s\n", err)
		}
	}()

	newValue, err := IncrBy(dbNum, [][]byte{args[0], []byte("1")}, dbOp)
	if err != nil {
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
	dbOp.chainDBOperation()
	defer func() {
		if !wasChained {
			dbOp.unchainDBOperation()
		}
		if err := dbOp.endDBOperation(); err != nil {
			utils.Print("Error while ending DB operation: %s\n", err)
		}
	}()

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

	if _, err := Set(dbNum, args, dbOp); err != nil {
		return 0, err
	}

	return newValue, nil
}

func GetSet(dbNum int, args [][]byte, dbOp *dbOperation) ([]byte, error) {
	dbOp, err := startDBOperation(dbOp, true)
	if err != nil {
		return nil, err
	}
	wasChained := dbOp.chainDBOperation()
	defer func() {
		if !wasChained {
			dbOp.unchainDBOperation()
		}
		if err := dbOp.endDBOperation(); err != nil {
			utils.Print("Error while ending DB operation: %s\n", err)
		}
	}()

	value, err := Get(dbNum, args, dbOp)
	if err != nil {
		return nil, err
	}

	if _, err := Set(dbNum, args, dbOp); err != nil {
		return nil, err
	}

	return value, nil
}

func Strlen(dbNum int, args [][]byte) (int, error) {
	dbOp, err := startDBOperation(nil, false)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err := dbOp.endDBOperation(); err != nil {
			utils.Print("Error while ending DB operation: %s\n", err)
		}
	}()

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
	defer func() {
		dbOp.unchainDBOperation()
		if err := dbOp.endDBOperation(); err != nil {
			utils.Print("Error while ending DB operation: %s\n", err)
		}
	}()

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

	if _, err := Set(dbNum, args, dbOp); err != nil {
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
	defer func() {
		dbOp.unchainDBOperation()
		if err := dbOp.endDBOperation(); err != nil {
			utils.Print("Error while ending DB operation: %s\n", err)
		}
	}()

	args[1] = []byte("-1")

	newValue, err := IncrBy(dbNum, args, dbOp)
	if err != nil {
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
	defer func() {
		dbOp.unchainDBOperation()
		if err := dbOp.endDBOperation(); err != nil {
			utils.Print("Error while ending DB operation: %s\n", err)
		}
	}()

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
	defer func() {
		if err := dbOp.endDBOperation(); err != nil {
			utils.Print("Error while ending DB operation: %s\n", err)
		}
	}()

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
	defer func() {
		if err := dbOp.endDBOperation(); err != nil {
			utils.Print("Error while ending DB operation: %s\n", err)
		}
	}()

	// insert all keys and values in one shot

	if _, err := dbOp.Txn.Exec(fmt.Sprintf(`
		INSERT INTO bigdis_%d (key, value, type) VALUES %s
		ON CONFLICT(key) DO UPDATE SET
			value = excluded.value,
			updated = current_timestamp`, dbNum, strings.Repeat("(?, ?, 's'),", len(args)/2-1)+"(?, ?, 's')"), anyArgs...); err != nil {
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
	defer func() {
		dbOp.unchainDBOperation()
		if err := dbOp.endDBOperation(); err != nil {
			utils.Print("Error while ending DB operation: %s\n", err)
		}
	}()

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

	return 1, nil
}

func SetNX(dbNum int, args [][]byte) (int, error) {
	result, err := MSetNX(dbNum, args)
	if err != nil {
		return 0, err
	}

	return result, nil
}
