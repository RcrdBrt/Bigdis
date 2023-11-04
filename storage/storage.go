package storage

import (
	"bigdis/config"
	"database/sql"
	_ "embed"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

var DB *sql.DB

var AvailableDBs = map[int]struct{}{}

//go:embed init.sql
var initSQL string

func Init() {
	var err error
	DB, err = sql.Open("sqlite3", fmt.Sprintf(
		"file:%s?_auto_vacuum=1&_journal_mode=%s&_synchronous=%s",
		config.Config.Storage.Path,
		config.Config.Storage.JournalMode,
		config.Config.Storage.Synchronous))
	if err != nil {
		panic(err)
	}

	_, err = DB.Exec(initSQL)
	if err != nil {
		panic(err)
	}

	// scan existing tables for available DBs
	rows, err := DB.Query("SELECT name FROM main.sqlite_schema WHERE type='table'")
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
	_, err := DB.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS bigdis_%d (
			key TEXT PRIMARY KEY,
			value BLOB NOT NULL,
			type TEXT NOT NULL,
			created datetime default current_timestamp,
			updated datetime default current_timestamp)`, dbNum))
	if err != nil {
		return err
	}

	return nil
}

func FlushDB(dbNum int) error {
	_, err := DB.Exec(fmt.Sprintf("DROP TABLE bigdis_%d", dbNum))
	if err != nil {
		return err
	}

	return nil
}

func Exists(dbNum int, args [][]byte, dbOp *dbOperation) (int, error) {
	dbOp, err := startDBOperation(dbOp)
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
	dbOp, err := startDBOperation(dbOp)
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
