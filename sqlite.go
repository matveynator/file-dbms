package filedbms

import (
	"database/sql"
	"fmt"
)

// OpenSQLite creates a queue backed by SQLite database handles.
//
// Important:
// The SQLite driver must be registered before this function is called.
// A typical production import is:
//
//	import _ "github.com/matveynator/file-dbms/drivers"
//
// The function opens one writer handle and readWorkers read handles.
// This shape follows SQLite strengths: one writer, many concurrent readers.
func OpenSQLite(databasePath string, readWorkers int) (*Queue, error) {
	if readWorkers <= 0 {
		return nil, fmt.Errorf("filedbms: readWorkers must be > 0")
	}

	writer, err := openSQLiteHandle(databasePath)
	if err != nil {
		return nil, err
	}

	readers := make([]*sql.DB, 0, readWorkers)
	for i := 0; i < readWorkers; i++ {
		reader, openErr := openSQLiteHandle(databasePath)
		if openErr != nil {
			_ = writer.Close()
			for _, db := range readers {
				_ = db.Close()
			}
			return nil, openErr
		}

		readers = append(readers, reader)
	}

	return startQueue(writer, readers), nil
}

// openSQLiteHandle applies conservative defaults for file-backed SQLite.
func openSQLiteHandle(databasePath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", databasePath)
	if err != nil {
		return nil, err
	}

	// A dedicated *sql.DB handle should keep a small internal pool.
	// Queue-level concurrency is controlled with goroutines/channels, not with
	// large DB pools hidden inside each handle.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}

	if _, err := db.Exec("PRAGMA journal_mode = WAL;"); err != nil {
		_ = db.Close()
		return nil, err
	}

	if _, err := db.Exec("PRAGMA busy_timeout = 5000;"); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}
