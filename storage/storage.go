package storage

import (
	"bigdis/config"
	"bigdis/utils"
	"database/sql"
	_ "embed"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

/*
In SQLite, if the first operation of a transaction is a read operation,
then the transaction is deferred until the first write operation.
This is a known limitation of SQLite done to respect the full ACID properties
and could cause "database is locked" errors if there are multiple connections
racing to do a deferred transaction.
Converting the transaction to a BEGIN IMMEDIATE and setting a busy timeout
is not enough to solve it.
The choice to workaround this issue is between:
  - giving up on the atomicity and don't use transactions for read and then write transactions
  - use a separate connection pool limited to 1 connection for the database writes
  - recycle the same general pool, but limit the number of connections to 1
  - create a dummy write operation before the read operation to force the transaction to be immediate

Here it's been chosen to separate the read and write operations in two different pools.
This adds a slight bit of complexity to the code, but it's the most efficient solution with a balanced tradeoff.
*/

// DBwp is the DB write pool
var DBwp *sql.DB

// DBrp is the DB read pool
var DBrp *sql.DB

var AvailableDBs = map[int]struct{}{}

//go:embed init.sql
var initSQL string

func Init() {
	connString := fmt.Sprintf(
		"file:%s?_auto_vacuum=1&_journal_mode=%s&_synchronous=%s&_busy_timeout=20000&_tx_lock=immediate",
		config.Config.Storage.Path,
		config.Config.Storage.JournalMode,
		config.Config.Storage.Synchronous)

	if config.Config.Storage.Path == ":memory:" {
		connString += "&mode=memory&cache=shared"
	}

	var err error
	DBwp, err = sql.Open("sqlite3", connString)
	if err != nil {
		panic(err)
	}
	DBwp.SetMaxOpenConns(1)

	DBrp, err = sql.Open("sqlite3", connString)
	if err != nil {
		panic(err)
	}

	_, err = DBwp.Exec(initSQL)
	if err != nil {
		panic(err)
	}

	// scan existing tables for available DBs
	rows, err := DBrp.Query("SELECT name FROM main.sqlite_schema WHERE type='table' and name like 'bigdis_%'")
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			panic(err)
		}

		var dbNum int
		if _, err := fmt.Sscanf(tableName, "bigdis_%d", &dbNum); err != nil {
			continue
		}

		AvailableDBs[dbNum] = struct{}{}
	}

	// print detected DBs
	var detectedDBs []int
	for dbNum := range AvailableDBs {
		detectedDBs = append(detectedDBs, dbNum)
	}
	if len(detectedDBs) == 0 {
		fmt.Println("No DB detected, initializing...")
	} else {
		fmt.Printf("Detected non-empty DBs: %v\n", detectedDBs)
	}
}

func NewDB(dbNum int) error {
	_, err := DBwp.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS bigdis_%d (
			id INTEGER PRIMARY KEY,
			key TEXT UNIQUE NOT NULL,
			value BLOB NOT NULL,
			type TEXT NOT NULL,
			created datetime default current_timestamp,
			updated datetime default current_timestamp)`, dbNum))
	if err != nil {
		return err
	}

	return nil
}

func FlushDB(dbNum int, args [][]byte) error {
	dbOp, err := startDBOperation(nil, true)
	if err != nil {
		return err
	}

	sync := true
	if len(args) > 0 {
		if strings.ToLower(string(args[0])) == "async" {
			sync = false
		} else if strings.ToLower(string(args[0])) != "sync" {
			return utils.ErrWrongSyntax
		}
	}

	if sync {
		if _, err = dbOp.Txn.Exec(fmt.Sprintf("DROP TABLE bigdis_%d", dbNum)); err != nil {
			if err.Error() != fmt.Sprintf("no such table: bigdis_%d", dbNum) {
				return err
			}
		}

		if err := dbOp.endDBOperation(); err != nil {
			return err
		}

		return nil
	}

	go func() {
		if _, err = dbOp.Txn.Exec(fmt.Sprintf("DROP TABLE bigdis_%d", dbNum)); err != nil {
			utils.Print("Error while dropping table: %s\n", err)
		}

		if err := dbOp.endDBOperation(); err != nil {
			utils.Print("Error while ending DB operation: %s\n", err)
		}
	}()

	return nil
}

func Exists(dbNum int, args [][]byte, dbOp *dbOperation) (int, error) {
	dbOp, err := startDBOperation(dbOp, false)
	if err != nil {
		return 0, err
	}

	var count int
	for i := range args {
		var exists bool
		if err := dbOp.Txn.QueryRow(fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM bigdis_%d WHERE key = ?)", dbNum), args[i]).Scan(&exists); err != nil {
			return 0, err
		}

		if exists {
			count++
		}
	}

	if err := dbOp.endDBOperation(); err != nil {
		return 0, err
	}

	return count, nil
}

func Del(dbNum int, args [][]byte, dbOp *dbOperation) (int, error) {
	dbOp, err := startDBOperation(dbOp, true)
	if err != nil {
		return 0, err
	}

	var deleted int
	for i := range args {
		if _, err := dbOp.Txn.Exec(fmt.Sprintf("DELETE FROM bigdis_%d WHERE key = ?", dbNum), args[i]); err != nil {
			return 0, err
		}

		deleted += 1
	}

	if err := dbOp.endDBOperation(); err != nil {
		return 0, err
	}

	return deleted, nil
}

func FlushAll(args [][]byte) error {
	dbOp, err := startDBOperation(nil, true)
	if err != nil {
		return err
	}

	sync := true
	if len(args) > 0 {
		if strings.ToLower(string(args[0])) == "async" {
			sync = false
		} else if strings.ToLower(string(args[0])) != "sync" {
			return utils.ErrWrongSyntax
		}
	}

	if sync {
		for dbNum := range AvailableDBs {
			if _, err = dbOp.Txn.Exec(fmt.Sprintf("DROP TABLE bigdis_%d", dbNum)); err != nil {
				if err.Error() != fmt.Sprintf("no such table: bigdis_%d", dbNum) {
					return err
				}
			}
		}

		if err := dbOp.endDBOperation(); err != nil {
			return err
		}

		return nil
	}

	go func() {
		for dbNum := range AvailableDBs {
			if _, err = dbOp.Txn.Exec(fmt.Sprintf("DROP TABLE bigdis_%d", dbNum)); err != nil {
				utils.Print("Error while dropping table: %s\n", err)
			}
		}

		if err := dbOp.endDBOperation(); err != nil {
			utils.Print("Error while ending DB operation: %s\n", err)
		}
	}()

	return nil
}
