# Write-Ahead Log (WAL) Design

This document explains the design decisions behind the WAL implementation.

---

## Design Goals

The WAL exists to:
- preserve mutation order
- ensure durability
- remain independent of protocol and storage internals

---

## Single-Writer Invariant

Exactly one goroutine owns the WAL file.

All writes are funneled through:
- an unbuffered request channel
- a dedicated worker goroutine

This guarantees:
- no concurrent file access
- deterministic write ordering
- simpler correctness model

---

## Append Semantics

Append consists of:
1. encode record
2. write to file
3. fsync (depending on policy)
4. acknowledge caller

Encoding is done outside the worker to:
- parallelize CPU work
- keep worker focused on IO

---

## Replay Semantics

Replay:
- reads WAL sequentially
- decodes line-by-line
- stops at first corrupt record

Rationale:
- partial writes can occur during crashes
- all records before corruption are trusted
- anything after is discarded

This mirrors real-world systems like Redis AOF.

---

## Rotation

Rotation exists to:
- prevent unbounded WAL growth
- enable snapshot + compaction

Rotation:
- is serialized through the worker
- preserves write ordering
- establishes a clean durability boundary

---

## What WAL Does Not Do

- does not interpret protocol
- does not validate business logic
- does not mutate memory

It records *intent only*.
