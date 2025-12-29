package store

import "time"

/*
operation represents the type of request sent to the event loop.
Each operation corresponds to one DataStore method.
*/
type operation int

const (
	opRead operation = iota
	opWrite
	opExpire
)

/*
request describes an intent to operate on the store.
It is sent by caller goroutines to the event loop goroutine.

Only the event loop goroutine is allowed to execute the request
and touch the underlying store.
*/
type request struct {
	op    operation
	key   string
	value Entry
	mode  PutMode
	ttl   time.Duration

	// reply is a per-request response channel used to return
	// results back to the caller synchronously.
	reply chan response
}

/*
response carries the result of executing a request.
A single response type is used for all operations:
- Read uses value + ok
- Write uses err
- Expire uses ok
*/
type response struct {
	value Entry
	ok    bool
	err   error
}

/*
eventLoopStore implements DataStore using a single-threaded
event loop model.

Exactly one goroutine owns the underlying store and processes
all requests sequentially. This eliminates the need for locks
and guarantees linearizable behavior across all operations.
*/
type eventLoopStore struct {
	requests chan request
}

/*
NewEventloopStore creates a new EventLoopStore with a bounded
request channel. The buffer size controls backpressure:
callers block when the channel is full.

The underlying store is owned exclusively by the event loop
goroutine and is never accessed directly by callers.
*/
func NewEventloopStore(buffer int) DataStore {
	reqCh := make(chan request, buffer)
	s := &store{
		data: make(map[string]Entry),
	}

	eLS := &eventLoopStore{
		requests: reqCh,
	}

	// Start the event loop goroutine which owns the store.
	go eLS.loop(s)

	return eLS
}

/*
loop runs in a dedicated goroutine and serially processes
all incoming requests.

This goroutine is the sole owner of the underlying store,
which guarantees safety without locks.
*/
func (s *eventLoopStore) loop(store *store) {
	for req := range s.requests {
		switch req.op {

		case opRead:
			entry, ok := store.Read(req.key)
			req.reply <- response{
				value: entry,
				ok:    ok,
			}

		case opWrite:
			err := store.Write(req.key, req.value, req.mode)
			req.reply <- response{
				err: err,
			}

		case opExpire:
			ok := store.Expire(req.key, req.ttl)
			req.reply <- response{
				ok: ok,
			}
		}
	}
}

/*
Read sends a read request to the event loop and blocks
until the operation is completed.

From the caller's perspective, this behaves like a
synchronous method call, even though the implementation
is message-based.
*/
func (s *eventLoopStore) Read(key string) (Entry, bool) {
	reply := make(chan response, 1)

	s.requests <- request{
		op:    opRead,
		key:   key,
		reply: reply,
	}

	resp := <-reply
	return resp.value, resp.ok
}

/*
Write sends a write request to the event loop and blocks
until the operation completes.
*/
func (s *eventLoopStore) Write(key string, value Entry, mode PutMode) error {
	reply := make(chan response, 1)

	s.requests <- request{
		op:    opWrite,
		key:   key,
		value: value,
		mode:  mode,
		reply: reply,
	}

	resp := <-reply
	return resp.err
}

/*
Expire sends an expiry request to the event loop and blocks
until the TTL metadata is updated.

Expired keys are removed lazily during reads, but all expiry
decisions are serialized through the event loop.
*/
func (s *eventLoopStore) Expire(key string, ttl time.Duration) bool {
	reply := make(chan response, 1)

	s.requests <- request{
		op:    opExpire,
		key:   key,
		ttl:   ttl,
		reply: reply,
	}

	resp := <-reply
	return resp.ok
}
