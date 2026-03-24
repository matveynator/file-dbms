# file-dbms Wiki: Usage Guide

This page shows practical usage patterns for `github.com/matveynator/file-dbms`.

## 1. Install

```bash
go get github.com/matveynator/file-dbms
```

## 2. Basic SQLite Example

```go
package main

import (
	"fmt"
	"log"

	filedbms "github.com/matveynator/file-dbms"
	_ "github.com/matveynator/file-dbms/drivers"
)

func main() {
	queue, err := filedbms.OpenSQLite("example.db", 4)
	if err != nil {
		log.Fatal(err)
	}
	defer queue.Close()

	jobs := queue.Jobs()

	createTableResult := <-filedbms.SubmitWrite(jobs,
		`CREATE TABLE IF NOT EXISTS metrics (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			value INTEGER NOT NULL
		)`,
	)
	if createTableResult.Err != nil {
		log.Fatal(createTableResult.Err)
	}

	insertResult := <-filedbms.SubmitWrite(jobs,
		`INSERT INTO metrics(name, value) VALUES (?, ?)`,
		"requests_total", 42,
	)
	if insertResult.Err != nil {
		log.Fatal(insertResult.Err)
	}

	readResult := <-filedbms.SubmitRead(jobs,
		`SELECT id, name, value FROM metrics ORDER BY id`,
	)
	if readResult.Err != nil {
		log.Fatal(readResult.Err)
	}

	fmt.Printf("rows=%v\n", readResult.Rows)
}
```

## 3. Conceptual Model

- `SubmitWrite` always goes to one serialized write worker.
- `SubmitRead` is distributed across N read workers.
- Every task carries its own one-shot reply channel.
- Reply send is non-blocking to avoid deadlocks caused by slow consumers.

## 4. Driver Imports

`OpenSQLite` and `OpenDuckDB` expect that the corresponding `database/sql` driver is already registered.

For SQLite:

```go
import _ "github.com/matveynator/file-dbms/drivers"
```

For DuckDB build (optional):

```bash
CGO_ENABLED=1 go build -tags duckdb
```

## 5. Error Handling Rules

- Always check `res.Err` from every reply.
- Use buffered reply channels (the helpers already do this).
- Always call `queue.Close()` via `defer` in short-lived programs.

## 6. Practical Tuning

- Start with `readWorkers = runtime.NumCPU()` for read-heavy workloads.
- For write-heavy workloads, increasing read workers usually gives little benefit.
- Keep SQL portable and engine-neutral when possible.
