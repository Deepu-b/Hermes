package wal

import "time"

/*
walOperation represents internal commands sent to the WAL worker.

The worker goroutine owns the WAL file exclusively.
All file IO is serialized through this channel-based protocol,
avoiding locks around file operations.
*/
type walOperation int

const (
	opAppend walOperation = iota
	opClose
	opSync
)

/*
request represents a single unit of work for the WAL worker.

payload is already encoded before reaching the worker so the
worker remains a pure IO executor with no domain logic.
*/
type request struct {
	payload   string
	operation walOperation

	reply chan response
}

type response struct {
	err error
}

/*
run is the WAL event loop.

Exactly one goroutine executes this function.
It provides:
- ordered writes
- fsync correctness
- no concurrent file access

This mirrors the event-loop approach used by Redis for persistence.
*/
func (w *wal) run() {
	var ticker <-chan time.Time
	if w.batchDuration > 0 {
		t := time.NewTicker(w.batchDuration)
		defer t.Stop()
		ticker = t.C
	}

	for {
		select {
		case req := <-w.reqChan:
			switch req.operation {
			case opAppend:
				err := w.append(req.payload)
				// check for synchronous writes vis fsync
				if w.batchDuration == 0 && err == nil {
					err = w.sync()
				}

				req.reply <- response{
					err: err,
				}

			case opClose:
				// Flush any remaining buffered data before dying
				_ = w.sync()
				err := w.close()
				req.reply <- response{
					err: err,
				}
				return

			case opSync:
				err := w.sync()
				req.reply <- response{
					err: err,
				}
			}

		case <-ticker:
			_ = w.sync()
		}
	}
}

/*
append writes a single encoded record to disk.
*/
func (w *wal) append(payload string) error {
	_, err := w.file.WriteString(payload)
	return err
}

/*
close closes the WAL file.
After this point, no further writes are permitted.
*/
func (w *wal) close() error {
	return w.file.Close()
}

/*
sync syncs the file to disk
*/
func (w *wal) sync() error {
	return w.file.Sync()
}
