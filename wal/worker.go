package wal

/*
walOperation represents internal commands sent to the WAL worker.

The worker goroutine owns the WAL file exclusively.
All file IO is serialized through this channel-based protocol,
avoiding locks around file operations.
*/
type walOpeation int

const (
	opAppend walOpeation = iota
	opClose
)

/*
request represents a single unit of work for the WAL worker.

payload is already encoded before reaching the worker so the
worker remains a pure IO executor with no domain logic.
*/
type request struct {
	payload   string
	operation walOpeation

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
	for req := range w.reqChan {
		switch req.operation {

		case opAppend:
			err := w.append(req.payload)
			req.reply <- response{
				err: err,
			}

		case opClose:
			err := w.close()
			req.reply <- response{
				err: err,
			}
			return
		}
	}
}

/*
append writes a single encoded record to disk and fsyncs it.

fsync is intentionally done per record to guarantee durability.
This sacrifices throughput for correctness, which is the correct
tradeoff at this stage of the system.
*/
func (w *wal) append(payload string) error {
	if _, err := w.file.WriteString(payload); err != nil {
		return err
	}

	return w.file.Sync()
}

/*
close flushes all pending data and closes the WAL file.

After this point, no further writes are permitted.
*/
func (w *wal) close() error {
	if err := w.file.Sync(); err != nil {
		return err
	}

	return w.file.Close()
}
