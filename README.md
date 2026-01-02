# Hermes

Hermes is a learning-oriented **in-memory key-value store** written in **Go**.

The project focuses on correctness, simplicity, and understanding system design
tradeoffs rather than production readiness or feature completeness.

---

## What Hermes Supports

- In-memory key-value storage
- Multiple write semantics (overwrite, insert-only, update-only)
- Key expiration using TTL
- Lazy expiration (expired keys are removed on access)
- Safe concurrent access

---

## Concurrency Models

Hermes exposes the same API using different concurrency approaches:

- **Global lock**  
  Simple and correct, but serializes all operations.

- **Sharded locks**  
  Reduces contention by partitioning keys across independent shards.

- **Single-threaded event loop**  
  One goroutine owns all state; operations are serialized via message passing.

Each model has different performance and reasoning tradeoffs, but identical
observable behavior.

---

## Networking
- TCP server using Go’s `net` package
- One goroutine per client connection
- Line-based request/response protocol
- Graceful connection lifecycle handling
- Read and write timeouts to protect against slow or stalled clients
- Input size limits to prevent unbounded memory usage

---

## Protocol & Command Layer
- Explicit command parsing and validation
- Central command registry with argument type checking
- Clear separation:
  - parse → validate → execute → respond
- Structured server responses:
  - OK
  - value
  - nil
  - client error
  - server error

---

## Design Notes

- Expiration is enforced lazily to keep the core simple.
- Reads may mutate state due to lazy expiration.
- Concurrency is handled outside the core store logic.
- All implementations follow the same correctness contract.
- Protocol parsing is decoupled from execution.
- Networking code does not assume datastore implementation details.
- Resource protection (timeouts, buffer limits) is enforced at the server layer.

---

## Explicit Non-Goals

- Binary-safe payloads
- Command pipelining
- Transactions
- Persistence or crash recovery
- Distributed or replicated operation

These are intentionally excluded.

---

## Scope

- Single-node
- In-memory only
- No persistence
- No eviction policies
- Standard library only
- TCP-based access

---

---

## Testing

- Unit tests cover storage, protocol parsing, command execution, and responses
- Integration tests cover server lifecycle and client interaction
- All tests pass under the Go race detector

Some test cases were created with the assistance of AI tools and then reviewed
and adapted to match Hermes’ correctness guarantees and design decisions.

---

## Status

Active development.  
The architecture evolves incrementally as new constraints are explored.
