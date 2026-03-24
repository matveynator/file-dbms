package filedbms

import (
	"database/sql"
	"errors"
	"fmt"
)

// Package filedbms provides a minimal channel-driven dispatcher for file-based
// SQL engines.
//
// The package core is driver-agnostic.
// Database-specific constructors live in separate files guarded by build tags.

// -----------------------------------------------------------------------------
// Public model
// -----------------------------------------------------------------------------

// Mode describes how a task must be executed.
type Mode int

const (
	// Read runs a query in the read pool.
	Read Mode = iota

	// Write runs a query in the single serialized write lane.
	Write
)

// Task is the minimal unit of work accepted by Queue.
//
// Reply should normally be a buffered channel with capacity 1.
// The queue performs a single non-blocking send attempt.
// If the send cannot proceed immediately, the result is dropped.
type Task struct {
	Mode  Mode
	Query string
	Args  []any
	Reply chan Result
}

// Result is the generic outcome of a task.
//
// Read tasks fill Rows.
// Write tasks fill Affected and LastInsertID when supported by the driver.
type Result struct {
	Rows         []map[string]any
	Affected     int64
	LastInsertID int64
	Err          error
}

// Queue routes incoming tasks into one write worker and many read workers.
type Queue struct {
	in     chan Task
	readQ  chan Task
	writeQ chan Task

	stop chan struct{}
	done chan struct{}

	closers []func() error
}

// Jobs returns the public input channel for tasks.
func (q *Queue) Jobs() chan<- Task {
	return q.in
}

// Close stops the queue and closes all owned database handles.
func (q *Queue) Close() error {
	select {
	case <-q.done:
		return nil
	default:
	}

	close(q.stop)
	<-q.done

	var joined error
	for _, closeFn := range q.closers {
		if err := closeFn(); err != nil {
			joined = errors.Join(joined, err)
		}
	}

	return joined
}

// SubmitRead creates and submits a one-shot read task.
func SubmitRead(jobs chan<- Task, query string, args ...any) <-chan Result {
	reply := make(chan Result, 1)

	jobs <- Task{
		Mode:  Read,
		Query: query,
		Args:  args,
		Reply: reply,
	}

	return reply
}

// SubmitWrite creates and submits a one-shot write task.
func SubmitWrite(jobs chan<- Task, query string, args ...any) <-chan Result {
	reply := make(chan Result, 1)

	jobs <- Task{
		Mode:  Write,
		Query: query,
		Args:  args,
		Reply: reply,
	}

	return reply
}

// startQueue builds the internal pipeline.
//
// The core pipeline is intentionally independent from any specific SQL engine.
func startQueue(writerDB *sql.DB, readerDBs []*sql.DB) *Queue {
	q := &Queue{
		in:      make(chan Task),
		readQ:   make(chan Task),
		writeQ:  make(chan Task),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
		closers: make([]func() error, 0, 1+len(readerDBs)),
	}

	q.closers = append(q.closers, writerDB.Close)
	for _, db := range readerDBs {
		db := db
		q.closers = append(q.closers, db.Close)
	}

	writeDone := make(chan struct{})
	readDone := make(chan struct{}, len(readerDBs))

	// router moves public tasks into dedicated internal lanes.
	go func() {
		defer close(q.readQ)
		defer close(q.writeQ)

		for {
			select {
			case <-q.stop:
				return

			case task, ok := <-q.in:
				if !ok {
					return
				}

				if task.Reply == nil {
					continue
				}

				switch task.Mode {
				case Read:
					select {
					case <-q.stop:
						return
					case q.readQ <- task:
					}

				case Write:
					select {
					case <-q.stop:
						return
					case q.writeQ <- task:
					}

				default:
					tryReply(task.Reply, Result{
						Err: fmt.Errorf("filedbms: unsupported task mode: %d", task.Mode),
					})
				}
			}
		}
	}()

	// writer serializes all write transactions.
	go func() {
		defer close(writeDone)

		for task := range q.writeQ {
			executeWrite(writerDB, task)
		}
	}()

	// readers execute read tasks in parallel.
	for _, db := range readerDBs {
		db := db

		go func() {
			defer func() { readDone <- struct{}{} }()

			for task := range q.readQ {
				executeRead(db, task)
			}
		}()
	}

	// finalizer waits for worker shutdown after stop is requested.
	go func() {
		defer close(q.done)

		<-q.stop
		<-writeDone

		for range readerDBs {
			<-readDone
		}
	}()

	return q
}

// executeRead runs one read-only transaction.
func executeRead(db *sql.DB, task Task) {
	tx, err := db.BeginTx(nil, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		tryReply(task.Reply, Result{Err: err})
		return
	}
	defer func() {
		_ = tx.Rollback()
	}()

	rows, err := tx.Query(task.Query, task.Args...)
	if err != nil {
		tryReply(task.Reply, Result{Err: err})
		return
	}
	defer rows.Close()

	data, err := collectRows(rows)
	if err != nil {
		tryReply(task.Reply, Result{Err: err})
		return
	}

	if err := tx.Commit(); err != nil {
		tryReply(task.Reply, Result{Err: err})
		return
	}

	tryReply(task.Reply, Result{Rows: data})
}

// executeWrite runs one serialized write transaction.
func executeWrite(db *sql.DB, task Task) {
	tx, err := db.Begin()
	if err != nil {
		tryReply(task.Reply, Result{Err: err})
		return
	}
	defer func() {
		_ = tx.Rollback()
	}()

	res, err := tx.Exec(task.Query, task.Args...)
	if err != nil {
		tryReply(task.Reply, Result{Err: err})
		return
	}

	if err := tx.Commit(); err != nil {
		tryReply(task.Reply, Result{Err: err})
		return
	}

	affected, _ := res.RowsAffected()
	lastInsertID, _ := res.LastInsertId()

	tryReply(task.Reply, Result{
		Affected:     affected,
		LastInsertID: lastInsertID,
	})
}

// tryReply performs exactly one non-blocking send attempt.
func tryReply(reply chan Result, result Result) {
	if reply == nil {
		return
	}

	defer func() {
		_ = recover()
	}()

	select {
	case reply <- result:
	default:
	}
}

// collectRows converts sql.Rows into a generic portable form.
func collectRows(rows *sql.Rows) ([]map[string]any, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	out := make([]map[string]any, 0)

	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))

		for i := range values {
			ptrs[i] = &values[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}

		row := make(map[string]any, len(cols))
		for i, col := range cols {
			row[col] = normalizeValue(values[i])
		}

		out = append(out, row)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

// normalizeValue converts driver byte slices into strings for text-like values.
func normalizeValue(v any) any {
	switch x := v.(type) {
	case []byte:
		return string(x)
	default:
		return x
	}
}
