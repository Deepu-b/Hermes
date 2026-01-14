# Crash Recovery

This document explains how Hermes recovers from crashes.

---

## Crash Model

Hermes assumes:
- process may crash at any time
- disk writes may be partially completed
- memory state is lost

---

## Recovery Algorithm

On startup:

1. Load snapshot (if present)
2. Replay WAL sequentially
3. Stop at first corrupt record
4. Serve traffic

---

## Corruption Handling

If WAL contains:
- partial line
- malformed record

Recovery:
- stops immediately
- trusts all prior records
- ignores remaining bytes

This avoids applying ambiguous state.

---

## Guarantees After Recovery

After recovery:
- state reflects last durable write
- ordering is preserved
- no phantom writes exist

Recovery never invents data.
