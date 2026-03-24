# file-dbms Wiki: Architecture

## Main Design Goal

Provide deterministic and simple concurrency for file-based SQL engines with explicit channel-based control.

## Pipeline

1. Public producer sends `Task` to `Queue.Jobs()`.
2. Router goroutine dispatches task to one of two internal channels:
   - `writeQ` for serialized writes
   - `readQ` for parallel reads
3. Worker goroutines execute SQL in transactions.
4. Worker sends one `Result` into task reply channel (non-blocking).

## Why One Writer

Many embedded/file SQL engines naturally prefer one writer at a time. A single writer lane avoids hidden lock contention and preserves write order.

## Why Multiple Readers

Read queries usually scale with independent DB handles. Pooling readers through dedicated goroutines makes throughput predictable and simple.

## Why Non-blocking Reply

A stuck consumer must not freeze workers forever. Non-blocking reply keeps the system moving and bounds queue-side backpressure behavior.

## Package Layout

- `queue.go`: core task model and dispatch pipeline.
- `sqlite.go`: SQLite constructor and handle defaults.
- `duckdb.go`: optional DuckDB constructor (build-tag guarded).
- `drivers/`: optional driver registration imports.
