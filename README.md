# file-dbms

`file-dbms` A pure Go library that provides an embedded file-based DBMS layer with concurrent read/write access and support for pluggable SQL backends

Import path:

```go
import "github.com/matveynator/file-dbms"
```

## Why this library

- One serialized write lane for deterministic write order.
- Many parallel read workers for throughput.
- Synchronization via goroutines, channels, and `select` only.
- Database integration through `database/sql` for engine portability.

## Quick start (SQLite)

```go
package main

import (
	"fmt"
	"log"

	filedbms "github.com/matveynator/file-dbms"
	_ "github.com/matveynator/file-dbms/drivers"
)

func main() {
	q, err := filedbms.OpenSQLite("example.db", 4)
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	jobs := q.Jobs()

	create := filedbms.SubmitWrite(
		jobs,
		`CREATE TABLE IF NOT EXISTS items (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL
		)`,
	)
	if res := <-create; res.Err != nil {
		log.Fatal(res.Err)
	}

	insert := filedbms.SubmitWrite(
		jobs,
		`INSERT INTO items(name) VALUES (?)`,
		"alpha",
	)
	if res := <-insert; res.Err != nil {
		log.Fatal(res.Err)
	}

	read := filedbms.SubmitRead(
		jobs,
		`SELECT id, name FROM items ORDER BY id`,
	)

	res := <-read
	if res.Err != nil {
		log.Fatal(res.Err)
	}

	fmt.Println(res.Rows)
}
```
