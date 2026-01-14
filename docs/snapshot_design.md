# Snapshot Design

Snapshots provide fast recovery by capturing a consistent in-memory state.

---

## Why Snapshots Exist

Without snapshots:
- WAL grows unbounded
- recovery time grows linearly with uptime

Snapshots trade:
- short pauses
for
- faster restarts

---

## Snapshot Consistency Model

Hermes uses a **stop-the-world** snapshot.

During snapshot:
- all writes are blocked
- readers may proceed
- state is fully consistent

This simplifies correctness and avoids copy-on-write complexity.

---

## Snapshot Contents

Each snapshot item contains:
- key
- value
- expiration timestamp

Expired keys are excluded.

---

## Atomicity

Snapshots are written to a temporary file and then atomically renamed.

This guarantees:
- either old snapshot exists
- or new snapshot exists
- never a partially written snapshot

---

## Snapshot + WAL Interaction

Snapshots establish a baseline.

After snapshot:
- WAL is rotated
- new WAL contains only newer mutations

Recovery uses:
- snapshot first
- WAL second
