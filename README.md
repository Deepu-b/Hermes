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

## Design Notes

- Expiration is enforced lazily to keep the core simple.
- Reads may mutate state due to lazy expiration.
- Concurrency is handled outside the core store logic.
- All implementations follow the same correctness contract.

---

## Scope

- Single-node
- In-memory only
- No persistence
- No eviction policies
- Standard library only

---

---

## Development Notes

Some test cases were created with the assistance of AI tools and
then reviewed and adapted to match Hermes’ correctness guarantees
and design decisions.

## Status

Active development.  
The architecture evolves incrementally as new constraints are explored.
