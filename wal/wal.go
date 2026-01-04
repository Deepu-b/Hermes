package wal

import (
	"bufio"
	"errors"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	// ErrWALClosed is returned when appending to a closed WAL.
	ErrWALClosed = errors.New("wal is closed")

	// ErrWorkerStuck protects against a wedged worker goroutine.
	// This is a safety guard, not a correctness mechanism.
	ErrWorkerStuck = errors.New("wal worker stuck")
)

/*
WAL defines an append-only write-ahead log for durable mutations.

Key properties:
- append-only
- ordered
- synchronous durability
- protocol-agnostic

The WAL records intent (SET, EXPIRE), not internal state.
*/
type WAL interface {
	Append(record WALRecord) error
	Replay(apply func(WALRecord) error) error
	Close() error
}

/*
wal is a single-writer WAL implementation.

Concurrency model:
- many goroutines may call Append; exactly one goroutine owns the file
- Multiple Producers (Append callers) -> Single Consumer (run goroutine).
- Ordering is guaranteed by the channel; writes are serialized FIFO.
- Durability is guaranteed by unbuffered channel hand-off (request-response).

This design avoids lock-heavy IO and keeps durability logic simple.
*/
type wal struct {
	// path is persisted to allow Replay to re-open the file on recovery.
	path string

	// file is kept open for the lifetime of the WAL to amortize syscall overhead.
	file *os.File

	// reqChan is UNBUFFERED; forces the caller to wait until the worker
	// acknowledges the write (fsync), ensuring no data is lost in a
	// user-space buffer during a crash.
	reqChan chan request

	// doneChan acts as a broadcast signal (tombstone) to notify all writers
	// that the WAL is shutting down.
	doneChan chan struct{}

	// closeOnce ensures the teardown logic is idempotent and thread-safe.
	closeOnce sync.Once
}

/*
NewWAL initializes a WAL backed by an append-only file.

Flags used:
- O_APPEND: Ensures writes always land at the end, preventing accidental overwrites.
- O_DSYNC (Optional consideration): We rely on explicit Sync() calls instead for batching flexibility.
*/
func NewWAL(path string) (WAL, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	wal := &wal{
		path:     path,
		file:     f,
		reqChan:  make(chan request), // unbuffered, ie, every write waits for fsync inside (handshake) = Strong Consistency
		doneChan: make(chan struct{}),
	}

	go wal.run()
	return wal, nil
}

/*
Append durably records a mutation.

Callers block until the record is:
- written
- fsynced
- acknowledged

Encoding happens here (in the caller's goroutine), not in the worker.
This increases throughput by parallelizing the CPU-intensive serialization,
leaving the single-threaded worker free to focus solely on I/O syscalls.
*/
func (w *wal) Append(record WALRecord) error {
	payload, err := EncodeRecord(record)
	if err != nil {
		return err
	}

	reply := make(chan response, 1)

	select {
	case w.reqChan <- request{
		operation: opAppend,
		payload:   payload,
		reply:     reply,
	}:
		// Block until the worker confirms fsync completion
		resp := <-reply
		return resp.err

	case <-w.doneChan:
		// Fast-path: If WAL is closed, don't even try to send request
		return ErrWALClosed
	}
}

/*
Close flushes and gracefully shuts down the WAL.

Reliability Features:
 1. Idempotency: Can be called safely by multiple goroutines (sync.Once).
 2. Timeout Guard: Uses the "Circuit Breaker" pattern to prevent hanging
    if the internal worker is unresponsive.
*/
func (w *wal) Close() error {
	// prevent from running Close() logic more than once
	closed := false

	// close the channel and notify via doneChan only ONCE
	w.closeOnce.Do(func() {
		closed = true
		// Broadcast shutdown signal immediately to unblock pending Appends
		close(w.doneChan)
	})

	if !closed {
		return nil
	}

	reply := make(chan response, 1)

	// time.After() gives given time to complete request, if not
	// then worker is assumed as stuck or dead
	// 1 second here arbitrary hence its a safety guard, not a guarantee.
	select {
	case w.reqChan <- request{
		operation: opClose,
		reply:     reply,
	}:
		resp := <-reply
		return resp.err

	case <-time.After(1 * time.Second):
		return ErrWorkerStuck
	}
}

/*
Replay reconstructs the state by iterating sequentially over the log.

Performance Note:
This is a blocking operation meant to run during the "Cold Start" phase.
It does not use the worker goroutine as the system is not yet concurrent.
*/
func (w *wal) Replay(apply func(WALRecord) error) error {
	file, err := os.Open(w.path)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		rec, err := DecodeRecord(line)
		if err != nil {
			return err
		}

		if err := apply(rec); err != nil {
			return err
		}
	}
	return scanner.Err()
}
