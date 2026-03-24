//go:build cgo && duckdb

package filedbms

import (
	"database/sql"
	"fmt"
)

// OpenDuckDB creates a queue backed by DuckDB handles.
//
// Driver requirements:
//   - import _ "github.com/matveynator/file-dbms/drivers"
//   - build with: CGO_ENABLED=1 and -tags duckdb
func OpenDuckDB(databasePath string, readWorkers int) (*Queue, error) {
	if readWorkers <= 0 {
		return nil, fmt.Errorf("filedbms: readWorkers must be > 0")
	}

	writer, err := openDuckDBHandle(databasePath)
	if err != nil {
		return nil, err
	}

	readers := make([]*sql.DB, 0, readWorkers)
	for i := 0; i < readWorkers; i++ {
		reader, openErr := openDuckDBHandle(databasePath)
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

func openDuckDBHandle(databasePath string) (*sql.DB, error) {
	db, err := sql.Open("duckdb", databasePath)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}
