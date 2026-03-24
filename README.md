# file-dbms

`file-dbms` is a Go library that provides a channel-driven queue over `database/sql`
for file-oriented SQL engines.

## Installation

```bash
go get github.com/matveynator/file-dbms
```

## Driver registration

Import the lightweight drivers package in your binary to register SQL drivers.

```go
import _ "github.com/matveynator/file-dbms/drivers"
```

The `drivers` directory is the extension point for additional file-based SQL backends.
New engines can be added with build tags without changing the queue core.

## Usage

### SQLite quick start

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

	createResult := <-filedbms.SubmitWrite(
		jobs,
		`CREATE TABLE IF NOT EXISTS items (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL
		)`,
	)
	if createResult.Err != nil {
		log.Fatal(createResult.Err)
	}

	insertResult := <-filedbms.SubmitWrite(
		jobs,
		`INSERT INTO items(name) VALUES (?)`,
		"alpha",
	)
	if insertResult.Err != nil {
		log.Fatal(insertResult.Err)
	}

	readResult := <-filedbms.SubmitRead(
		jobs,
		`SELECT id, name FROM items ORDER BY id`,
	)
	if readResult.Err != nil {
		log.Fatal(readResult.Err)
	}

	fmt.Println(readResult.Rows)
}
```

### Generic constructor

Use `Open` when you need to control driver name and DSN directly.

```go
queue, err := filedbms.Open(filedbms.OpenConfig{
	DriverName:   "sqlite",
	DataSource:   "example.db",
	ReadReplicas: 4,
})
```

## Build tags

- SQLite registration is enabled by platform-specific tags in `drivers/sqlite.go`.
- DuckDB registration requires CGO and the `duckdb` build tag.

Example:

```bash
CGO_ENABLED=1 go build -tags duckdb
```
