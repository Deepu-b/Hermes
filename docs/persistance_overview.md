# Persistence Overview

Hermes is an in-memory key-value store with durability guarantees.
Persistence is implemented to survive process crashes while preserving
correctness and ordering semantics.

This document explains *what is persisted*, *how recovery works*,
and *what guarantees are provided*.

---

## Why Persistence Exists

In-memory speed is useless if all state is lost on crash.

Persistence ensures:
- committed writes are not lost
- system can restart into a consistent state
- failures are bounded and understandable

Hermes explicitly targets **single-node durability**, not replication
or high availability.

---

## Persistence Components

Hermes uses two persistence mechanisms:

1. **Write-Ahead Log (WAL)**
2. **Periodic Snapshot**

These work together to balance:
- write latency
- recovery time
- disk usage

---

## Write-Ahead Log (WAL)

The WAL is an append-only log of *intent*, not internal state.

Each mutating operation records:
- SET key value
- EXPIRE key timestamp

### Key Properties

- Append-only
- Ordered
- Single-writer
- Protocol-agnostic
- Synchronous durability (configurable)

The WAL is the **source of truth**.

---

## Snapshot

A snapshot is a point-in-time dump of all live in-memory entries.

Properties:
- Taken under stop-the-world lock
- Includes TTL metadata
- Excludes expired keys
- Written atomically

Snapshots exist to:
- bound WAL replay time
- reduce recovery latency
- cap WAL growth

---

## Recovery Order

On startup, Hermes performs recovery in this strict order:

1. Load snapshot (if present)
2. Replay WAL from beginning
3. Accept new writes

This ensures:
- snapshot establishes a clean baseline
- WAL replays only newer mutations
- ordering is preserved

---

## Non-Goals

Persistence in Hermes does NOT provide:
- replication
- consensus
- distributed durability
- multi-node recovery

Those are explicitly out of scope.
