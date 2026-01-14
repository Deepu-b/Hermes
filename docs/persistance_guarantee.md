# Persistence Guarantees

This document defines the exact durability guarantees provided by Hermes.

Durability is not binary. It depends on configuration and failure mode.

---

## WAL Sync Policies

Hermes supports multiple WAL sync policies.

### SyncEveryWrite

- fsync is performed after every write
- a write is acknowledged only after data reaches disk

Guarantees:
- acknowledged writes survive process crashes
- acknowledged writes survive OS crashes
- acknowledged writes survive power loss (assuming disk honors fsync)

Tradeoff:
- higher write latency

---

### SyncEverySecond (Batching)

- fsync is performed periodically (e.g. every 1s)
- writes may be acknowledged before reaching disk

Guarantees:
- writes become durable after next fsync
- ordering between writes is preserved

Failure Risk:
- acknowledged writes may be lost if crash occurs before fsync

Tradeoff:
- lower latency
- higher throughput
- weaker durability window

---

## Failure Scenarios

### Process Crash

- WAL ensures replay up to last durable record
- writes after last fsync may be lost (depending on policy)

### OS Crash / Power Loss

- only fsynced WAL records are guaranteed
- snapshot durability depends on explicit fsync

### Disk Full

- writes fail explicitly
- no silent corruption is allowed

---

## What Hermes Does NOT Guarantee

- durability of un-fsynced writes
- atomicity across multiple keys
- linearizability across crashes

These are intentional tradeoffs.
